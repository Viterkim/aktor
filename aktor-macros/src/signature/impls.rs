use super::*;
use crate::names;

impl VisitMut for Borrows<'_> {
    fn visit_type_reference_mut(&mut self, reference: &mut syn::TypeReference) {
        if reference.lifetime.is_none() {
            reference.lifetime = Some(parse_quote!('_));
        }

        visit_mut::visit_type_reference_mut(self, reference);
    }

    fn visit_lifetime_mut(&mut self, lifetime: &mut syn::Lifetime) {
        if lifetime.ident == "_" {
            let name = names::binding(self.reserved, "__aktor_borrow");
            *lifetime = syn::Lifetime::new(&format!("'{name}"), lifetime.apostrophe);
            self.lifetimes.push(parse_quote!(#lifetime));
        }
    }

    fn visit_type_fn_ptr_mut(&mut self, _: &mut syn::TypeFnPtr) {}

    fn visit_parenthesized_generic_arguments_mut(
        &mut self,
        _: &mut syn::ParenthesizedGenericArguments,
    ) {
    }
}

impl VisitMut for Inputs<'_> {
    fn visit_type_mut(&mut self, ty: &mut Type) {
        if let Type::ImplTrait(opaque) = ty {
            visit_mut::visit_type_impl_trait_mut(self, opaque);

            let name = names::binding(self.reserved, "__AktorInput");
            let bounds = &opaque.bounds;

            self.parameters.push(parse_quote!(#name: #bounds));
            *ty = parse_quote!(#name);
        } else {
            visit_mut::visit_type_mut(self, ty);
        }
    }
}

impl VisitMut for StaticOutput {
    fn visit_type_impl_trait_mut(&mut self, ty: &mut syn::TypeImplTrait) {
        visit_mut::visit_type_impl_trait_mut(self, ty);

        if !ty.bounds.iter().any(|bound| matches!(bound, TypeParamBound::Lifetime(lifetime) if lifetime.ident == "static")) {
            ty.bounds.push(parse_quote!('static));
        }
    }
}

impl VisitMut for Captures {
    fn visit_type_impl_trait_mut(&mut self, ty: &mut syn::TypeImplTrait) {
        visit_mut::visit_type_impl_trait_mut(self, ty);

        if let Some(TypeParamBound::PreciseCapture(capture)) = ty
            .bounds
            .iter_mut()
            .find(|bound| matches!(bound, TypeParamBound::PreciseCapture(_)))
        {
            for added in &self.added {
                capture.params.push(parse_quote!(#added));
            }
        } else {
            let captures = &self.captures;
            ty.bounds.push(parse_quote!(use<#(#captures),*>));
        }
    }
}

impl VisitMut for DispatchOutput {
    fn visit_type_impl_trait_mut(&mut self, ty: &mut syn::TypeImplTrait) {
        visit_mut::visit_type_impl_trait_mut(self, ty);

        ty.bounds = ty
            .bounds
            .iter()
            .filter(|bound| !matches!(bound, TypeParamBound::PreciseCapture(_)))
            .cloned()
            .collect();
    }
}
