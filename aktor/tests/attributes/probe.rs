extern crate proc_macro;

use proc_macro::{Delimiter, Group, TokenStream, TokenTree};

#[proc_macro_attribute]
pub fn trace(_: TokenStream, item: TokenStream) -> TokenStream {
    item.into_iter()
        .map(|token| {
            if let TokenTree::Group(body) = &token
                && body.delimiter() == Delimiter::Brace
            {
                let mut output: TokenStream =
                    "crate::VISITS.fetch_add(1, ::std::sync::atomic::Ordering::Relaxed);"
                        .parse()
                        .unwrap();

                output.extend(body.stream());

                let mut group = Group::new(Delimiter::Brace, output);

                group.set_span(body.span());
                TokenTree::Group(group)
            } else {
                token
            }
        })
        .collect()
}
