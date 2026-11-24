use proc_macro2::{Span, TokenStream};
use quote::quote;
use syn::{DeriveInput, Lifetime, Path, Type, parse_quote};

pub(crate) fn expand(input: DeriveInput) -> syn::Result<TokenStream> {
    let mut transfer_type = None;
    let mut crate_path: Option<Path> = None;
    for attribute in &input.attrs {
        if !attribute.path().is_ident("temporal") {
            continue;
        }
        attribute.parse_nested_meta(|meta| {
            if meta.path.is_ident("transfer_type") {
                if transfer_type.is_some() {
                    return Err(meta.error("duplicate transfer_type"));
                }
                transfer_type = Some(meta.value()?.parse::<Type>()?);
                Ok(())
            } else if meta.path.is_ident("crate") {
                if crate_path.is_some() {
                    return Err(meta.error("duplicate crate path"));
                }
                crate_path = Some(meta.value()?.parse()?);
                Ok(())
            } else {
                Err(meta.error("expected transfer_type or crate"))
            }
        })?;
    }
    let transfer_type = transfer_type.ok_or_else(|| {
        syn::Error::new_spanned(
            &input.ident,
            "TemporalTransfer requires #[temporal(transfer_type = Type)]",
        )
    })?;
    let crate_path = crate_path.unwrap_or_else(|| parse_quote!(::temporalio_common));
    let dc = quote!(#crate_path::data_converters);
    let name = &input.ident;
    let (_, type_generics, _) = input.generics.split_for_impl();
    let model = quote!(#name #type_generics);

    let mut lifetime_name = "'__temporal_transfer".to_owned();
    while input
        .generics
        .lifetimes()
        .any(|parameter| parameter.lifetime.to_string() == lifetime_name)
    {
        lifetime_name.push('_');
    }
    let lifetime = Lifetime::new(&lifetime_name, Span::mixed_site());
    let mut generics = input.generics.clone();
    let predicates = &mut generics.make_where_clause().predicates;
    predicates.push(parse_quote!(
        #model: ::core::convert::TryFrom<#transfer_type>
    ));
    predicates.push(parse_quote!(
        #transfer_type: #dc::TemporalSerializable
            + #dc::TemporalDeserializable
            + for<#lifetime> ::core::convert::TryFrom<&#lifetime #model>
            + 'static
    ));
    predicates.push(parse_quote!(
        <#model as ::core::convert::TryFrom<#transfer_type>>::Error:
            ::std::error::Error + ::core::marker::Send + ::core::marker::Sync + 'static
    ));
    predicates.push(parse_quote!(
        for<#lifetime>
        <#transfer_type as ::core::convert::TryFrom<&#lifetime #model>>::Error:
            ::std::error::Error + ::core::marker::Send + ::core::marker::Sync + 'static
    ));
    let (impl_generics, _, where_clause) = generics.split_for_impl();

    Ok(quote! {
        impl #impl_generics #dc::HasTransferType for #model #where_clause {
            type Transfer = #transfer_type;
        }

        impl #impl_generics #dc::TemporalSerializable for #model #where_clause {
            fn to_transfer_value(
                &self,
            ) -> ::core::result::Result<
                ::core::option::Option<#dc::__private::TransferValue>,
                #dc::PayloadConversionError,
            > {
                #dc::__private::to_transfer_value(self).map(::core::option::Option::Some)
            }
        }

        impl #impl_generics #dc::TemporalDeserializable for #model #where_clause {
            fn transfer_decoder(
            ) -> ::core::option::Option<#dc::__private::TransferDecoder<Self>> {
                ::core::option::Option::Some(#dc::__private::TransferDecoder::for_model())
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_invalid_attributes() {
        for (input, expected) in [
            (
                quote!(
                    struct Model;
                ),
                "TemporalTransfer requires #[temporal(transfer_type = Type)]",
            ),
            (
                quote!(
                    #[temporal(unknown = String)]
                    struct Model;
                ),
                "expected transfer_type or crate",
            ),
            (
                quote!(
                    #[temporal(transfer_type = String, transfer_type = u64)]
                    struct Model;
                ),
                "duplicate transfer_type",
            ),
            (
                quote!(
                    #[temporal(transfer_type = String, crate = first, crate = second)]
                    struct Model;
                ),
                "duplicate crate path",
            ),
        ] {
            let error = expand(syn::parse2(input).unwrap()).unwrap_err();
            assert_eq!(error.to_string(), expected);
        }
    }

    #[test]
    fn transfer_models_compile() {
        let tests = trybuild::TestCases::new();
        tests.pass("tests/temporal_transfer/pass.rs");
        tests.compile_fail("tests/temporal_transfer/*_fail.rs");
    }
}
