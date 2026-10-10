use proc_macro2::TokenStream;
use std::collections::HashSet;
use syn::{
    Type, TypeParam, TypeParamBound, parse_quote,
    visit_mut::{self, VisitMut},
};

pub mod body;
pub mod impls;
pub mod prepare;
pub mod scope;
pub use body::implementation;
pub use prepare::{arguments, dispatch_output, inputs, normalize, output};

pub struct Inputs<'a> {
    pub reserved: &'a mut HashSet<String>,
    pub parameters: Vec<TypeParam>,
}

pub struct StaticOutput;

pub struct Captures {
    pub captures: Vec<TokenStream>,
    pub added: Vec<TokenStream>,
}

pub struct DispatchOutput;

pub struct Borrows<'a> {
    pub reserved: &'a mut HashSet<String>,
    pub lifetimes: Vec<syn::LifetimeParam>,
}
