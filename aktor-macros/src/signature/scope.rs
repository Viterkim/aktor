use syn::{
    Path, parse_quote,
    visit_mut::{self, VisitMut},
};

/// The same visibility seen from inside the generated module. A plain `pub` in there claims
/// more than a private function has, and the compiler then refuses private traits in the
/// types that item mentions.
pub fn nested_visibility(visibility: &syn::Visibility) -> syn::Visibility {
    match visibility {
        syn::Visibility::Public(_) => visibility.clone(),
        syn::Visibility::Inherited => parse_quote!(pub(super)),
        syn::Visibility::Restricted(restricted) => {
            let mut path = (*restricted.path).clone();

            if path.is_ident("self") {
                path = parse_quote!(super);
            } else if path
                .segments
                .first()
                .is_some_and(|first| first.ident == "super")
            {
                path.segments.insert(0, parse_quote!(super));
            }

            parse_quote!(pub(in #path))
        }
    }
}

pub struct NestedModule {
    pub shadowed: &'static [&'static str],
    pub binders: Vec<String>,
}
impl NestedModule {
    pub fn new(shadowed: &'static [&'static str], generics: &syn::Generics) -> Self {
        Self {
            shadowed,
            binders: generics
                .params
                .iter()
                .filter_map(|parameter| match parameter {
                    syn::GenericParam::Type(parameter) => Some(&parameter.ident),
                    syn::GenericParam::Const(parameter) => Some(&parameter.ident),
                    syn::GenericParam::Lifetime(_) => None,
                })
                .map(|name| name.to_string().trim_start_matches("r#").to_owned())
                .collect(),
        }
    }
}
impl VisitMut for NestedModule {
    fn visit_type_path_mut(&mut self, ty: &mut syn::TypePath) {
        if let Some(qself) = &mut ty.qself
            && qself.position == 0
        {
            self.visit_qself_mut(qself);
            visit_mut::visit_path_mut(self, &mut ty.path);
            return;
        }

        let before = ty.path.segments.len();

        visit_mut::visit_type_path_mut(self, ty);

        if let Some(qself) = &mut ty.qself {
            qself.position += ty.path.segments.len() - before;
        }
    }

    fn visit_expr_path_mut(&mut self, expr: &mut syn::ExprPath) {
        if let Some(qself) = &mut expr.qself
            && qself.position == 0
        {
            self.visit_qself_mut(qself);
            visit_mut::visit_path_mut(self, &mut expr.path);
            return;
        }

        let before = expr.path.segments.len();

        visit_mut::visit_expr_path_mut(self, expr);

        if let Some(qself) = &mut expr.qself {
            qself.position += expr.path.segments.len() - before;
        }
    }

    fn visit_path_mut(&mut self, path: &mut Path) {
        visit_mut::visit_path_mut(self, path);

        if path.leading_colon.is_some() {
            return;
        }

        let Some(first) = path.segments.first_mut() else {
            return;
        };

        let written = first.ident.to_string();
        let name = written.trim_start_matches("r#");

        if name == "self" {
            first.ident = syn::Ident::new("super", first.ident.span());
        } else if name == "super"
            || (self.shadowed.contains(&name) && !self.binders.iter().any(|binder| binder == name))
        {
            path.segments.insert(0, parse_quote!(super));
        }
    }
}
