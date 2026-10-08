use super::Shape;
use proc_macro2::TokenStream;
use std::collections::HashSet;
use syn::{Generics, Ident, Type, parse_quote, visit::Visit, visit_mut::VisitMut};

struct Mentions<'a> {
    names: &'a HashSet<String>,
    found: bool,
}
impl<'ast> Visit<'ast> for Mentions<'_> {
    fn visit_ident(&mut self, ident: &'ast Ident) {
        if self
            .names
            .contains(ident.to_string().trim_start_matches("r#"))
        {
            self.found = true;
        }
    }
}

pub fn add_bounds(generics: &mut Generics, name: &Ident, shapes: &[Shape], aktor: &TokenStream) {
    let parameter_names: Vec<_> = generics
        .type_params()
        .map(|parameter| parameter.ident.clone())
        .collect();
    let parameters: HashSet<_> = parameter_names
        .iter()
        .map(|name| name.to_string().trim_start_matches("r#").to_owned())
        .collect();

    let recursive = HashSet::from([
        name.to_string().trim_start_matches("r#").to_owned(),
        "Self".to_owned(),
    ]);
    let mut recursive_parameters = HashSet::new();

    for field in shapes.iter().flat_map(|shape| &shape.members) {
        let ty = &field.ty;

        if field.skip {
            generics
                .make_where_clause()
                .predicates
                .push(parse_quote!(#ty: ::core::default::Default));
            continue;
        }

        let mut uses_parameter = Mentions {
            names: &parameters,
            found: false,
        };
        uses_parameter.visit_type(ty);

        if !uses_parameter.found {
            continue;
        }

        let mut uses_self = Mentions {
            names: &recursive,
            found: false,
        };
        uses_self.visit_type(ty);

        if uses_self.found {
            for parameter in &parameters {
                let mut mentioned = Mentions {
                    names: &HashSet::from([parameter.clone()]),
                    found: false,
                };
                mentioned.visit_type(ty);

                if mentioned.found {
                    recursive_parameters.insert(parameter.clone());
                }
            }
        } else {
            generics
                .make_where_clause()
                .predicates
                .push(parse_quote!(#ty: #aktor::AktorData));
        }
    }

    for parameter in parameter_names {
        if !recursive_parameters.contains(parameter.to_string().trim_start_matches("r#")) {
            continue;
        }

        generics
            .make_where_clause()
            .predicates
            .push(parse_quote!(#parameter: #aktor::AktorData));
    }
}

pub struct NormalizeSelf<'a>(pub &'a Type);
impl VisitMut for NormalizeSelf<'_> {
    fn visit_type_path_mut(&mut self, ty: &mut syn::TypePath) {
        syn::visit_mut::visit_type_path_mut(self, ty);

        if ty.qself.is_none()
            && ty
                .path
                .segments
                .first()
                .is_some_and(|part| part.ident == "Self")
        {
            let original = self.0;

            if ty.path.segments.len() == 1 {
                *ty = parse_quote!(#original);
            } else {
                let rest = syn::Path {
                    leading_colon: None,
                    segments: ty.path.segments.iter().skip(1).cloned().collect(),
                };
                *ty = parse_quote!(<#original>::#rest);
            }
        }
    }

    fn visit_expr_path_mut(&mut self, expr: &mut syn::ExprPath) {
        syn::visit_mut::visit_expr_path_mut(self, expr);

        if expr.qself.is_none()
            && expr
                .path
                .segments
                .first()
                .is_some_and(|part| part.ident == "Self")
            && expr.path.segments.len() > 1
        {
            let original = self.0;
            let rest = syn::Path {
                leading_colon: None,
                segments: expr.path.segments.iter().skip(1).cloned().collect(),
            };
            *expr = parse_quote!(<#original>::#rest);
        }
    }
}

pub struct RenameLifetime<'a>(pub &'a syn::Lifetime);
impl VisitMut for RenameLifetime<'_> {
    fn visit_lifetime_mut(&mut self, lifetime: &mut syn::Lifetime) {
        if lifetime.ident == "de" {
            *lifetime = self.0.clone();
        }
    }
}
