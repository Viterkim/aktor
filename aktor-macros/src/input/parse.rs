use super::*;
use syn::{
    FnArg, ItemFn,
    spanned::Spanned,
    visit::{self, Visit},
};

impl Function {
    pub fn new(function: ItemFn) -> syn::Result<Self> {
        if function.sig.asyncness.is_none() {
            return Err(syn::Error::new(
                function.sig.fn_token.span,
                "#[aktor] functions must be async",
            ));
        }

        let mut output = OutputVisitor { error: None };
        output.visit_return_type(&function.sig.output);

        if let Some(error) = output.error {
            return Err(error);
        }

        let mut arguments = function.sig.inputs.iter();
        let first = arguments
            .next()
            .ok_or_else(|| syn::Error::new(function.sig.span(), "missing actor state"))?;
        state(first)?;

        let inputs = arguments.map(argument).collect::<syn::Result<_>>()?;

        Ok(Self {
            attributes: function.attrs,
            visibility: function.vis,
            signature: function.sig,
            body: function.block,
            inputs,
        })
    }
}

pub fn state(input: &FnArg) -> syn::Result<State> {
    let argument = typed(input)?;
    let pattern = (*argument.pat).clone();

    let Type::Reference(reference) = argument.ty.as_ref() else {
        return Err(syn::Error::new(
            argument.ty.span(),
            "actor state must be &T or &mut T",
        ));
    };

    Ok(State {
        pattern,
        ty: (*reference.elem).clone(),
        mutable: reference.mutability.is_some(),
    })
}

pub fn argument(input: &FnArg) -> syn::Result<Argument> {
    let argument = typed(input)?;

    Ok(Argument {
        pattern: (*argument.pat).clone(),
    })
}

pub fn typed(input: &FnArg) -> syn::Result<&syn::PatType> {
    match input {
        FnArg::Typed(argument) => {
            if let Some(attribute) = argument.attrs.first() {
                return Err(syn::Error::new(
                    attribute.span(),
                    "parameter attributes are not supported by #[aktor]; put cfg on the function",
                ));
            }

            Ok(argument)
        }
        FnArg::Receiver(receiver) => Err(syn::Error::new(
            receiver.span(),
            "actor functions cannot take self",
        )),
    }
}

pub struct OutputVisitor {
    pub error: Option<syn::Error>,
}
impl<'a> Visit<'a> for OutputVisitor {
    fn visit_type_fn_ptr(&mut self, _: &'a syn::TypeFnPtr) {}

    fn visit_parenthesized_generic_arguments(&mut self, _: &'a syn::ParenthesizedGenericArguments) {
    }

    fn visit_type_reference(&mut self, reference: &'a syn::TypeReference) {
        if reference.lifetime.is_none() && self.error.is_none() {
            self.error = Some(syn::Error::new(
                reference.span(),
                "actor outputs cannot borrow the state; return owned data or an explicit 'static reference",
            ));
        }

        visit::visit_type_reference(self, reference);
    }
}
