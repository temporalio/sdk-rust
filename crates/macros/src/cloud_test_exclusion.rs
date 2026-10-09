use proc_macro2::TokenStream;
use quote::quote;
use syn::{
    Attribute, ItemFn, LitStr, Path, PathArguments, Token,
    parse::{Parse, ParseStream},
    parse_quote,
};

pub(crate) fn expand(attr: TokenStream, item: TokenStream) -> syn::Result<TokenStream> {
    let exclusion = syn::parse2::<CloudTestExclusion>(attr)?;
    let mut test_fn = syn::parse2::<ItemFn>(item)?;
    let reason = &exclusion.reason;
    let reason_type_check = quote!(const _: crate::CloudTestExclusionReason = #reason;);
    let test_attribute_index = test_fn
        .attrs
        .iter()
        .position(is_test_executor_attribute)
        .ok_or_else(|| {
            syn::Error::new_spanned(
                &test_fn.sig.ident,
                "cloud_test_exclusion can only be applied to a test function",
            )
        })?;

    if test_fn
        .attrs
        .iter()
        .any(|attribute| attribute.path().is_ident("ignore"))
    {
        return Ok(quote! {
            #reason_type_check
            #test_fn
        });
    }

    let ignore_reason = exclusion.ignore_reason();
    // Rstest treats attributes before a case as case-specific, but copies attributes next to the
    // test executor to every generated case.
    test_fn.attrs.insert(
        test_attribute_index,
        parse_quote!(#[cfg_attr(feature = "cloud-test-mode", ignore = #ignore_reason)]),
    );
    Ok(quote! {
        #reason_type_check
        #test_fn
    })
}

struct CloudTestExclusion {
    reason: Path,
    note: Option<LitStr>,
}

impl CloudTestExclusion {
    fn ignore_reason(&self) -> LitStr {
        let reason = self.reason.segments.last().unwrap().ident.to_string();
        let reason = match self.note.as_ref() {
            Some(note) => format!("{reason}: {}", note.value()),
            None => reason,
        };
        LitStr::new(&reason, self.reason.segments.last().unwrap().ident.span())
    }
}

impl Parse for CloudTestExclusion {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        let reason = input.parse::<Path>()?;
        if reason.leading_colon.is_some()
            || reason.segments.len() != 3
            || reason.segments[0].ident != "crate"
            || reason.segments[1].ident != "CloudTestExclusionReason"
            || reason
                .segments
                .iter()
                .any(|segment| !matches!(segment.arguments, PathArguments::None))
        {
            return Err(syn::Error::new_spanned(
                &reason,
                "expected crate::CloudTestExclusionReason::<Variant>",
            ));
        }
        let note = if input.peek(Token![,]) {
            input.parse::<Token![,]>()?;
            if input.is_empty() {
                None
            } else {
                let note = input.parse::<LitStr>()?;
                if note.value().trim().is_empty() {
                    return Err(syn::Error::new_spanned(
                        note,
                        "exclusion note cannot be empty",
                    ));
                }
                if input.peek(Token![,]) {
                    input.parse::<Token![,]>()?;
                }
                Some(note)
            }
        } else {
            None
        };
        if !input.is_empty() {
            return Err(input.error("unexpected cloud_test_exclusion argument"));
        }
        Ok(Self { reason, note })
    }
}

fn is_test_executor_attribute(attribute: &Attribute) -> bool {
    let path = attribute.path();
    path.is_ident("test")
        || (path.segments.len() == 2
            && path.segments.first().unwrap().ident == "tokio"
            && path.segments.last().unwrap().ident == "test")
}

#[cfg(test)]
mod tests {
    use super::*;
    use quote::quote;

    #[test]
    fn adds_note_to_ignore_reason() {
        let expanded = expand(
            quote!(
                crate::CloudTestExclusionReason::DoesNotUseServer,
                "Uses synthetic workflow history."
            ),
            quote! {
                #[tokio::test]
                async fn example() {}
            },
        )
        .unwrap()
        .to_string();

        assert!(expanded.contains("cloud-test-mode"));
        assert!(expanded.contains("DoesNotUseServer: Uses synthetic workflow history."));
        assert!(expanded.contains("const _ : crate :: CloudTestExclusionReason"));
    }

    #[test]
    fn accepts_reason_without_note() {
        let expanded = expand(
            quote!(crate::CloudTestExclusionReason::DoesNotUseServer),
            quote! {
                #[test]
                fn example() {}
            },
        )
        .unwrap()
        .to_string();

        assert!(expanded.contains("ignore = \"DoesNotUseServer\""));
    }

    #[test]
    fn puts_ignore_after_rstest_cases() {
        let expanded = expand(
            quote!(
                crate::CloudTestExclusionReason::RequiresCloudProvisioning,
                "Requires a configured search attribute."
            ),
            quote! {
                #[rstest::rstest]
                #[case(true)]
                #[case(false)]
                #[tokio::test]
                async fn example(#[case] value: bool) {}
            },
        )
        .unwrap();
        let expanded = syn::parse2::<syn::File>(expanded).unwrap();
        let syn::Item::Fn(expanded) = &expanded.items[1] else {
            panic!("second expanded item must remain the annotated test")
        };

        assert_eq!(
            expanded.attrs[0].path().segments.last().unwrap().ident,
            "rstest"
        );
        assert!(expanded.attrs[1].path().is_ident("case"));
        assert!(expanded.attrs[2].path().is_ident("case"));
        assert!(expanded.attrs[3].path().is_ident("cfg_attr"));
        assert!(is_test_executor_attribute(&expanded.attrs[4]));
    }

    #[test]
    fn preserves_permanently_ignored_test() {
        let expanded = expand(
            quote!(
                crate::CloudTestExclusionReason::RequiresLocalServer,
                "Runs only against the local test server."
            ),
            quote! {
                #[ignore = "Manual test"]
                #[test]
                fn example() {}
            },
        )
        .unwrap()
        .to_string();

        assert!(!expanded.contains("cfg_attr"));
        assert!(expanded.contains("Manual test"));
        assert!(expanded.contains("const _ : crate :: CloudTestExclusionReason"));
    }

    #[test]
    fn rejects_noncanonical_reason_path() {
        let error = expand(
            quote!(DoesNotUseServer),
            quote! {
                #[test]
                fn example() {}
            },
        )
        .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("expected crate::CloudTestExclusionReason::<Variant>")
        );
    }

    #[test]
    fn rejects_empty_note() {
        let error = expand(
            quote!(crate::CloudTestExclusionReason::NeedsCloudAdaptation, "  "),
            quote! {
                #[test]
                fn example() {}
            },
        )
        .unwrap_err();

        assert!(error.to_string().contains("exclusion note cannot be empty"));
    }
}
