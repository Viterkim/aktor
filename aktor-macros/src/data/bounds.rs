use super::Shape;
use proc_macro2::TokenStream;
use quote::ToTokens;
use std::collections::{HashMap, HashSet};
use syn::{Generics, Ident, Lifetime, Path, Type, ext::IdentExt, parse_quote, visit::Visit};

// A field called wire::Packet<T> might be this Packet through an import.
// Rust rejects a T used only recursively, so if that's the only place T
// occurs, wire::Packet must be a different type. Skipped fields count too.
type Module = (bool, Vec<Ident>);

struct ParameterUses<'a> {
    name: &'a Ident,
    parameters: &'a HashSet<Ident>,
    module: Option<Module>,
    only_in: HashMap<Ident, Option<Module>>,
}
impl ParameterUses<'_> {
    fn record(&mut self, ident: &Ident) {
        let previous = self
            .only_in
            .entry(ident.clone())
            .or_insert_with(|| self.module.clone());
        if *previous != self.module {
            *previous = None;
        }
    }
}
impl<'ast> Visit<'ast> for ParameterUses<'_> {
    fn visit_type_path(&mut self, ty: &'ast syn::TypePath) {
        if self.module.is_none() && ty.qself.is_none() {
            let path = &ty.path;
            if path.is_ident("Self") {
                return;
            }
            if path
                .segments
                .last()
                .is_some_and(|last| last.ident.unraw() == *self.name)
            {
                if path.segments.len() == 1 {
                    return;
                }
                self.module = Some(module(path));
                syn::visit::visit_type_path(self, ty);
                self.module = None;
                return;
            }
        }
        syn::visit::visit_type_path(self, ty);
    }

    fn visit_ident(&mut self, ident: &'ast Ident) {
        let ident = ident.unraw();
        if self.parameters.contains(&ident) {
            self.record(&ident);
        }
    }

    fn visit_lifetime(&mut self, _: &'ast Lifetime) {}

    fn visit_macro(&mut self, _: &'ast syn::Macro) {
        // Only rustc knows which types this expands to.
        for parameter in self.parameters {
            self.record(parameter);
        }
    }
}

fn module(path: &Path) -> Module {
    (
        path.leading_colon.is_some(),
        path.segments
            .iter()
            .take(path.segments.len() - 1)
            .map(|part| part.ident.unraw())
            .collect(),
    )
}

struct FieldBounds<'a, 'ast> {
    name: Ident,
    arguments: Vec<RecursiveArgument>,
    parameters: &'a HashSet<Ident>,
    foreign: HashSet<Module>,
    different: HashSet<String>,
    permutations: Vec<HashMap<String, Ident>>,
    unchanged: HashSet<String>,
    bounds: Vec<&'ast Type>,
    mentions: usize,
    recursive: usize,
}

struct RecursiveArgument {
    spelling: String,
    default_uses: HashSet<String>,
}

impl<'ast> FieldBounds<'_, 'ast> {
    fn recursive_arguments(&mut self, supplied: &syn::AngleBracketedGenericArguments) {
        if supplied
            .args
            .iter()
            .map(|value| value.to_token_stream().to_string().replace("r#", ""))
            .eq(self
                .arguments
                .iter()
                .map(|argument| argument.spelling.as_str()))
        {
            return;
        }

        // A recursive call with renamed type parameters needs the same field bounds with
        // those names substituted, including projections such as B::Item.
        if supplied.args.len() == self.arguments.len() {
            let renamed: Option<HashMap<_, _>> = self
                .arguments
                .iter()
                .zip(&supplied.args)
                .map(|(param, value)| {
                    let syn::GenericArgument::Type(Type::Path(path)) = value else {
                        return None;
                    };
                    let ident = path.path.get_ident()?.unraw();
                    self.parameters
                        .contains(&ident)
                        .then(|| (param.spelling.clone(), ident))
                })
                .collect();
            if let Some(renamed) = renamed {
                self.permutations.push(renamed);
                return;
            }
        }

        let mut uses = Vec::<HashSet<String>>::new();

        for (position, argument) in self.arguments.iter().enumerate() {
            let mut found = HashSet::new();
            let unchanged = if let Some(value) = supplied.args.get(position) {
                let tokens = value.to_token_stream();
                let unchanged = tokens.to_string().replace("r#", "") == argument.spelling;

                crate::names::collect(tokens, &mut found);
                unchanged
            } else {
                for (earlier, names) in self.arguments.iter().zip(&uses) {
                    if argument.default_uses.contains(&earlier.spelling) {
                        found.extend(names.iter().cloned());
                    }
                }

                false
            };

            if unchanged {
                self.unchanged.extend(found.iter().cloned());
            } else {
                self.different.extend(found.iter().cloned());
            }
            uses.push(found);
        }
    }

    fn parts(&mut self, ty: &'ast Type) {
        let before = self.bounds.len();
        let mentions = self.mentions;
        let recursive = self.recursive;
        syn::visit::visit_type(self, ty);

        if self.mentions != mentions && self.recursive == recursive {
            // Keep C::Value as the bound in (C::Value, Box<Self>).
            self.bounds.truncate(before);
            self.bounds.push(ty);
        }
    }
}
impl<'ast> Visit<'ast> for FieldBounds<'_, 'ast> {
    fn visit_type(&mut self, ty: &'ast Type) {
        if let Type::Path(path) = ty
            && path.qself.is_none()
            && let Some(last) = path.path.segments.last()
            && (path.path.is_ident("Self")
                || (last.ident.unraw() == self.name
                    && (path.path.segments.len() == 1
                        || !self.foreign.contains(&module(&path.path)))))
        {
            self.recursive += 1;

            if let syn::PathArguments::AngleBracketed(arguments) = &last.arguments {
                self.recursive_arguments(arguments);
            }
            return;
        }

        self.parts(ty);
    }

    fn visit_type_path(&mut self, ty: &'ast syn::TypePath) {
        let self_projection = if let Some(qself) = &ty.qself {
            matches!(&*qself.ty, Type::Path(ty) if ty.qself.is_none() && ty.path.is_ident("Self"))
        } else {
            ty.path.segments.len() > 1
                && ty
                    .path
                    .segments
                    .first()
                    .is_some_and(|part| part.ident == "Self")
        };
        if self_projection {
            self.mentions += 1;
            self.visit_path(&ty.path);
        } else {
            syn::visit::visit_type_path(self, ty);
        }
    }

    fn visit_ident(&mut self, ident: &'ast Ident) {
        if self.parameters.contains(&ident.unraw()) {
            self.mentions += 1;
        }
    }

    fn visit_macro(&mut self, _: &'ast syn::Macro) {
        self.mentions += 1;
    }
}

struct Rename<'a>(&'a HashMap<String, Ident>);
impl syn::visit_mut::VisitMut for Rename<'_> {
    fn visit_type_path_mut(&mut self, path: &mut syn::TypePath) {
        syn::visit_mut::visit_type_path_mut(self, path);
        if path.qself.is_none()
            && path.path.leading_colon.is_none()
            && let Some(first) = path.path.segments.first_mut()
            && let Some(ident) = self.0.get(&first.ident.unraw().to_string())
        {
            first.ident = ident.clone();
        }
    }
}

fn first_bound(ty: &Type, seen: &mut HashSet<String>) -> bool {
    let spelling = ty.to_token_stream().to_string();

    // A macro or const block can expand differently at two places in the enum.
    spelling.contains(['!', '$', '{']) || seen.insert(spelling)
}

fn renamed_bounds<'a>(
    bounds: impl Iterator<Item = &'a Type>,
    permutations: &[HashMap<String, Ident>],
) -> Vec<Type> {
    let mut pending: Vec<Type> = bounds.cloned().collect();
    let mut variants: HashSet<String> = pending
        .iter()
        .map(|ty| ty.to_token_stream().to_string())
        .collect();
    let mut index = 0;
    while index < pending.len() {
        for permutation in permutations {
            let mut ty = pending[index].clone();
            syn::visit_mut::VisitMut::visit_type_mut(&mut Rename(permutation), &mut ty);
            if variants.insert(ty.to_token_stream().to_string()) {
                pending.push(ty);
            }
        }
        index += 1;
    }

    pending
}

pub fn add_bounds(generics: &mut Generics, name: &Ident, shapes: &[Shape], aktor: &TokenStream) {
    let parameters: HashSet<_> = generics.type_params().map(|p| p.ident.unraw()).collect();
    let fields = || shapes.iter().flat_map(|shape| &shape.members);
    let name = name.unraw();
    let mut uses = ParameterUses {
        name: &name,
        parameters: &parameters,
        module: None,
        only_in: Default::default(),
    };
    for field in fields() {
        uses.visit_type(&field.ty);
    }
    let foreign = uses.only_in.into_values().flatten().collect();

    let arguments = generics
        .params
        .iter()
        .map(|parameter| {
            let (spelling, default) = match parameter {
                syn::GenericParam::Type(p) => (
                    p.ident.unraw().to_string(),
                    p.default
                        .as_ref()
                        .map(|(_, default)| default.to_token_stream()),
                ),
                syn::GenericParam::Lifetime(p) => (
                    p.lifetime.to_token_stream().to_string().replace("r#", ""),
                    None,
                ),
                syn::GenericParam::Const(p) => (
                    p.ident.unraw().to_string(),
                    p.default
                        .as_ref()
                        .map(|(_, default)| default.to_token_stream()),
                ),
            };
            let mut default_uses = HashSet::new();

            if let Some(default) = default {
                crate::names::collect(default, &mut default_uses);
            }

            RecursiveArgument {
                spelling,
                default_uses,
            }
        })
        .collect();
    let mut walk = FieldBounds {
        name,
        arguments,
        parameters: &parameters,
        foreign,
        different: HashSet::new(),
        permutations: Vec::new(),
        unchanged: HashSet::new(),
        bounds: Vec::new(),
        mentions: 0,
        recursive: 0,
    };
    for field in fields().filter(|field| !field.skip) {
        // The whole field can't be Self by value. Its arguments still can contain Self.
        walk.parts(&field.ty);
    }

    let skipped = fields().filter(|field| field.skip).map(|field| &field.ty);
    let mut defaults = HashSet::new();
    for ty in renamed_bounds(skipped, &walk.permutations)
        .iter()
        .filter(|ty| first_bound(ty, &mut defaults))
    {
        generics
            .make_where_clause()
            .predicates
            .push(parse_quote!(#ty: ::core::default::Default));
    }

    if parameters.is_empty() {
        return;
    }

    let pending = renamed_bounds(walk.bounds.iter().copied(), &walk.permutations);
    let mut seen = HashSet::new();
    let mut serialized = HashSet::new();
    for ty in pending.iter().filter(|ty| first_bound(ty, &mut seen)) {
        if !walk.unchanged.is_empty() {
            let mut used = HashSet::new();
            crate::names::collect(ty.to_token_stream(), &mut used);
            if !used.is_disjoint(&walk.different) {
                serialized.extend(used);
            }
        }

        generics
            .make_where_clause()
            .predicates
            .push(parse_quote!(#ty: #aktor::AktorData));
    }
    walk.different
        .extend(walk.unchanged.intersection(&serialized).cloned());

    let different: Vec<_> = generics
        .type_params()
        .map(|parameter| &parameter.ident)
        .filter(|ident| walk.different.contains(&ident.unraw().to_string()))
        .cloned()
        .collect();
    for parameter in different {
        generics
            .make_where_clause()
            .predicates
            .push(parse_quote!(#parameter: #aktor::AktorData));
    }
}
