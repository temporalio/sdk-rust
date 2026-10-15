use proc_macro2::TokenStream;
use quote::{quote, quote_spanned};
use syn::{Attribute, FnArg, ImplItem, ItemImpl, Meta, Pat, Path, Type, parse::Parser};

pub(crate) fn expand(args: TokenStream, item: TokenStream) -> syn::Result<TokenStream> {
    let mut service = None;
    syn::meta::parser(|meta| {
        if !meta.path.is_ident("service") {
            return Err(meta.error("expected `service = ServiceMarker`"));
        }
        if service.is_some() {
            return Err(meta.error("duplicate `service` argument"));
        }
        service = Some(meta.value()?.parse::<Path>()?);
        Ok(())
    })
    .parse2(args)?;
    let service = service.ok_or_else(|| {
        syn::Error::new(
            proc_macro2::Span::call_site(),
            "missing `service = ServiceMarker`",
        )
    })?;
    let mut implementation: ItemImpl = syn::parse2(item)?;
    if implementation.trait_.is_some() {
        return Err(syn::Error::new_spanned(
            &implementation,
            "Nexus service handlers require an inherent impl",
        ));
    }
    let mut registrations = Vec::new();
    for item in &mut implementation.items {
        let ImplItem::Fn(method) = item else {
            continue;
        };
        let markers: Vec<_> = method
            .attrs
            .iter()
            .filter(|attr| {
                attr.path()
                    .segments
                    .last()
                    .is_some_and(|segment| segment.ident == "temporal_operation")
            })
            .collect();
        if markers.is_empty() {
            continue;
        }
        if markers.len() > 1 {
            return Err(syn::Error::new_spanned(
                markers[1],
                "duplicate #[temporal_operation]",
            ));
        }
        if !matches!(markers[0].meta, Meta::Path(_)) {
            return Err(syn::Error::new_spanned(
                markers[0],
                "#[temporal_operation] does not accept arguments",
            ));
        }
        let signature = &method.sig;
        if signature.asyncness.is_none()
            || signature.unsafety.is_some()
            || signature.abi.is_some()
            || signature.constness.is_some()
            || signature.variadic.is_some()
            || !signature.generics.params.is_empty()
            || signature.generics.where_clause.is_some()
        {
            return Err(syn::Error::new_spanned(
                signature,
                "Nexus operations must be async methods without generics, const, unsafe, or extern",
            ));
        }
        let mut inputs = signature.inputs.iter();
        match inputs.next() {
            Some(FnArg::Receiver(receiver))
                if receiver.reference.is_some()
                    && receiver.mutability.is_none()
                    && receiver.colon_token.is_none() => {}
            _ => {
                return Err(syn::Error::new_spanned(
                    signature,
                    "Nexus operations require an &self receiver",
                ));
            }
        }
        let arguments = inputs.collect::<Vec<_>>();
        if !(2..=3).contains(&arguments.len()) {
            return Err(syn::Error::new_spanned(
                signature,
                "expected client and input, or client, context, and input after &self",
            ));
        }
        for argument in &arguments {
            if !matches!(argument, FnArg::Typed(_)) {
                return Err(syn::Error::new_spanned(
                    argument,
                    "expected a typed operation argument",
                ));
            }
        }
        let explicit_context = arguments.len() == 3;
        if explicit_context {
            let FnArg::Typed(context) = arguments[1] else {
                unreachable!()
            };
            if !matches!(&*context.ty, Type::Path(ty) if ty.qself.is_none()
                && ty.path.segments.last().is_some_and(|s| s.ident == "TemporalOperationStartContext"))
            {
                return Err(syn::Error::new_spanned(
                    &context.ty,
                    "the middle argument must be TemporalOperationStartContext",
                ));
            }
        } else {
            let FnArg::Typed(client) = arguments[0] else {
                unreachable!()
            };
            let Pat::Ident(client_pattern) = &*client.pat else {
                return Err(syn::Error::new_spanned(
                    &client.pat,
                    "the client argument must have an identifier binding",
                ));
            };
            if client_pattern.by_ref.is_some() || client_pattern.subpat.is_some() {
                return Err(syn::Error::new_spanned(
                    &client.pat,
                    "the client argument must have an identifier binding",
                ));
            }
            let client = &client_pattern.ident;
            // Use the caller's hygiene so the user's body can refer to this implicit binding.
            let context = syn::Ident::new("context", client.span());
            method.block.stmts.insert(
                0,
                syn::parse_quote! {
                    #[allow(unused_variables)]
                    let #context = #client.context().clone();
                },
            );
        }
        let name = &method.sig.ident;
        let conditions = conditional_attrs(&method.attrs);
        let context_argument = if explicit_context {
            quote! { context }
        } else {
            quote! { _context }
        };
        let call = if explicit_context {
            quote! { handler.#name(client, context, input).await }
        } else {
            quote! { handler.#name(client, input).await }
        };
        registrations.push(quote_spanned! {name.span()=>
            #(#conditions)*
            {
                let handler = ::std::sync::Arc::clone(&self);
                service_handler = service_handler.operation(
                    #service::#name,
                    move |client, #context_argument, input| {
                        let handler = ::std::sync::Arc::clone(&handler);
                        async move { #call }
                    },
                )?;
            }
        });
        method.attrs.retain(|attr| {
            attr.path()
                .segments
                .last()
                .is_none_or(|segment| segment.ident != "temporal_operation")
        });
    }
    if registrations.is_empty() {
        return Err(syn::Error::new_spanned(
            &implementation,
            "a Nexus service handler requires at least one #[temporal_operation] method",
        ));
    }
    let conditions = conditional_attrs(&implementation.attrs);
    let (impl_generics, _, where_clause) = implementation.generics.split_for_impl();
    let handler_type = &implementation.self_ty;
    Ok(quote! {
        #implementation

        #(#conditions)*
        impl #impl_generics
            ::temporalio_sdk::nexus::TemporalNexusServiceHandler
            for #handler_type #where_clause
        {
            type Service = #service;

            fn into_service_handler(self: ::std::sync::Arc<Self>) -> ::std::result::Result<
                ::temporalio_sdk::nexus::NexusServiceHandler<Self::Service>,
                ::temporalio_sdk::nexus::NexusRegistrationError,
            > {
                #[allow(unused_mut)]
                let mut service_handler =
                    ::temporalio_sdk::nexus::NexusServiceHandler::new(#service);
                #(#registrations)*
                ::std::result::Result::Ok(service_handler)
            }
        }
    })
}

fn conditional_attrs(attrs: &[Attribute]) -> Vec<&Attribute> {
    attrs
        .iter()
        .filter(|attr| attr.path().is_ident("cfg") || attr.path().is_ident("cfg_attr"))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use syn::Item;

    #[test]
    fn preserves_conditional_registration_helpers_and_generic_impl() {
        let item = quote! {
            #[cfg(feature = "handlers")]
            #[allow(dead_code)]
            impl<T> PaymentHandlers<T> where T: Send + Sync + 'static {
                fn helper(&self) {}

                #[cfg(feature = "charge")]
                #[cfg_attr(feature = "disable", cfg(any()))]
                #[allow(clippy::unused_async)]
                #[temporal_operation]
                async fn charge(&self, client: TemporalNexusClient, input: ChargeInput)
                    -> HandlerResult { make_result(input) }

                #[temporal_operation]
                async fn refund(&self, client: TemporalNexusClient, input: RefundInput)
                    -> HandlerResult { make_result(input) }
            }
        };
        let file: syn::File =
            syn::parse2(expand(quote!(service = payments::PaymentService), item).unwrap()).unwrap();
        let Item::Impl(original) = &file.items[0] else {
            panic!("expected original impl");
        };
        let Item::Impl(generated) = &file.items[1] else {
            panic!("expected generated impl");
        };
        assert_eq!(original.items.len(), 3);
        assert_eq!(original.attrs.len(), 2);
        assert_eq!(generated.attrs, vec![original.attrs[0].clone()]);
        assert_eq!(generated.generics, original.generics);
        assert_eq!(generated.self_ty, original.self_ty);
        let ImplItem::Fn(charge) = &original.items[1] else {
            panic!("expected charge");
        };
        assert_eq!(charge.attrs.len(), 3);
        let ImplItem::Fn(bindings) = &generated.items[1] else {
            panic!("expected registration method");
        };
        let syn::Stmt::Expr(syn::Expr::Block(registration), _) = &bindings.block.stmts[1] else {
            panic!("expected conditional registration block");
        };
        assert_eq!(registration.attrs, charge.attrs[..2]);
        assert_eq!(bindings.block.stmts.len(), 4);
    }

    #[test]
    fn rejects_invalid_service_arguments() {
        for args in [
            quote!(),
            quote!(unknown = PaymentService),
            quote!(service = PaymentService, service = OtherService),
        ] {
            assert!(
                expand(args.clone(), quote!(impl Handlers {})).is_err(),
                "{args}"
            );
        }
    }

    #[test]
    fn rejects_invalid_operation_signatures() {
        for method in [
            quote!(
                fn charge(&self, client: Client, input: Input) {}
            ),
            quote!(
                async fn charge(&mut self, client: Client, input: Input) {}
            ),
            quote!(
                async fn charge<T>(&self, client: Client, input: T) {}
            ),
            quote!(
                async unsafe fn charge(&self, client: Client, input: Input) {}
            ),
            quote!(
                async extern "C" fn charge(&self, client: Client, input: Input) {}
            ),
            quote!(
                async fn charge(&self, input: Input) {}
            ),
            quote!(
                async fn charge(&self, client: Client, context: Wrong, input: Input) {}
            ),
        ] {
            let item = quote! {
                impl Handlers {
                    #[temporal_operation]
                    #method
                }
            };
            assert!(expand(quote!(service = Service), item).is_err(), "{method}");
        }
    }

    #[test]
    fn rejects_trait_impls_missing_operations_and_marker_arguments() {
        for item in [
            quote!(impl Trait for Handlers {}),
            quote!(impl Handlers { fn helper() {} }),
            quote! {
                impl Handlers {
                    #[temporal_operation(name = "charge")]
                    async fn charge(&self, client: Client, input: Input) {}
                }
            },
            quote! {
                impl Handlers {
                    #[temporal_operation]
                    #[other::temporal_operation]
                    async fn charge(&self, client: Client, input: Input) {}
                }
            },
        ] {
            assert!(
                expand(quote!(service = Service), item.clone()).is_err(),
                "{item}"
            );
        }
    }
}
