use darling::{ast::Style, FromDeriveInput, FromField};
use proc_macro2::{Span, TokenStream, TokenTree};
use proc_macro_crate::FoundCrate;
use quote::{format_ident, quote, ToTokens};
use syn::{parse_macro_input, parse_quote, DeriveInput, GenericParam, Generics};

#[proc_macro_derive(RowFormat, attributes(row))]
pub fn derive_row_format(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let mut input = parse_macro_input!(input as DeriveInput);
    if let Err(err) = fix_field_attrs(&mut input) {
        return err.into_compile_error().into();
    }

    let parsed = match RowFormat::from_derive_input(&input) {
        Ok(parsed) => parsed,
        Err(err) => return err.write_errors().into(),
    };
    let builder = match RowFormatBuilder::new(parsed) {
        Ok(builder) => builder,
        Err(err) => return err.into_compile_error().into(),
    };
    builder.implement().into()
}

#[derive(Debug, Clone, FromDeriveInput)]
#[darling(attributes(row), supports(struct_any), forward_attrs)]
struct RowFormat {
    ident: syn::Ident,
    vis: syn::Visibility,
    generics: Generics,
    data: darling::ast::Data<(), Column>,
    #[darling(default)]
    builder: Option<syn::Ident>,
    #[darling(default)]
    view: Option<syn::Ident>,
}

#[derive(Debug, Clone, FromField)]
#[darling(attributes(row), forward_attrs)]
struct Column {
    ident: Option<syn::Ident>,
    ty: syn::Type,
    #[darling(default)]
    name: Option<String>,
    #[darling(default, rename = "r#type")]
    as_type: Option<syn::Type>,
}

struct ColumnBuilder {
    ident: TokenStream,
    ty: syn::Type,
    name: String,
}

impl ColumnBuilder {
    fn new(col: Column, num: TokenStream) -> Result<Self, syn::Error> {
        let ident = col
            .ident
            .as_ref()
            .map_or_else(|| num.clone(), |ident| ident.to_token_stream());
        let name = match (col.name, &col.ident) {
            (Some(name), _) => name,
            (None, Some(ident)) => ident.to_string(),
            _ => {
                return Err(syn::Error::new_spanned(
                    col.ty.clone(),
                    "missing field name".to_string(),
                ))
            }
        };
        let ty = col.as_type.unwrap_or(col.ty);
        Ok(Self { ident, ty, name })
    }
}

struct RowFormatBuilder {
    ident: syn::Ident,
    vis: syn::Visibility,
    generics: Generics,
    style: Style,
    crt: TokenStream,
    fields: Vec<ColumnBuilder>,
    view_name: syn::Ident,
    builder_name: syn::Ident,
}

impl RowFormatBuilder {
    fn new(input: RowFormat) -> Result<Self, syn::Error> {
        let crt = synapse_crate();
        let generics = Self::with_bounds(input.generics, &crt);
        let fields = input.data.take_struct().unwrap();
        let (style, fields) = fields.split();
        let fields = fields
            .iter()
            .enumerate()
            .map(|(i, f)| ColumnBuilder::new(f.clone(), syn::Index::from(i).to_token_stream()))
            .collect::<Result<Vec<_>, _>>()?;

        let view_name = input
            .view
            .unwrap_or_else(|| format_ident!("_{}View", input.ident));
        let builder_name = input
            .builder
            .unwrap_or_else(|| format_ident!("_{}Builder", input.ident));

        Ok(Self {
            ident: input.ident,
            vis: input.vis,
            generics,
            style,
            fields,
            crt,
            view_name,
            builder_name,
        })
    }

    fn implement(self) -> TokenStream {
        let crt = &self.crt;
        let row = quote! { #crt::common::row };
        let ident = &self.ident;
        let generics = &self.generics;
        let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

        let view_name = &self.view_name;
        let builder_name = &self.builder_name;

        let field_types = self.field_types();

        let impl_builder = self.impl_builder();
        let impl_view = self.impl_view();

        let num_cols = quote! {
            #(<#field_types as #row::RowFormat>::COLUMNS +)* 0
        };

        quote! {
            #[automatically_derived]
            impl #impl_generics #crt::common::row::RowFormat for #ident #ty_generics #where_clause {
                const COLUMNS: usize = #num_cols;
                type Builder = #builder_name #ty_generics;
                type View = #view_name #ty_generics;

                fn builder(fields: &[::std::sync::Arc<#crt::derive::Field>]) -> #crt::Result<Self::Builder> {
                    #builder_name::<#ty_generics>::new(fields)
                }

                fn view(rows: usize, fields: &[::std::sync::Arc<#crt::derive::Field>], arrays: &[#crt::derive::ArrayRef]) -> #crt::Result<Self::View> {
                    #view_name::<#ty_generics>::new(rows, fields, arrays)
                }
            }

            #impl_builder
            #impl_view
        }
    }

    fn impl_builder(&self) -> TokenStream {
        let crt = &self.crt;
        let row = quote! { #crt::common::row };
        let vis = &self.vis;
        let ident = &self.ident;
        let generics = &self.generics;
        let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

        let builder_name = &self.builder_name;
        let field_types = self.field_types();
        let field_idents = self.field_idents();
        let builder_fields = self
            .field_names()
            .into_iter()
            .map(|name| syn::Ident::new(&name, Span::call_site()))
            .collect::<Vec<_>>();

        let len = syn::Ident::new("_synapse_len", Span::call_site());
        let doc = format!("[`{}::RowBatchBuilder`] for [`{}`]", row, ident);

        quote! {
            #[doc = #doc]
            #[derive(Debug, Clone)]
            #vis struct #builder_name #generics {
                #len: usize,
                #(#builder_fields: <#field_types as #row::RowFormat>::Builder, )*
            }

            #[automatically_derived]
            impl #impl_generics #builder_name #ty_generics #where_clause {
                fn new(mut fields: &[::std::sync::Arc<#crt::derive::Field>]) -> #crt::Result<#builder_name #ty_generics> {
                    if fields.len() != <#ident #ty_generics as #row::RowFormat>::COLUMNS {
                        return Err(#crt::Error::ColumnCount(<#ident #ty_generics as #row::RowFormat>::COLUMNS, fields.len()));
                    }

                    #(
                        let cols = <#field_types as #row::RowFormat>::COLUMNS;
                        let #builder_fields = <#field_types as #row::RowFormat>::builder(&fields[..cols])?;
                        fields = &fields[cols..];
                    )*

                    Ok(#builder_name {
                        #len: 0,
                        #(#builder_fields, )*
                    })
                }
            }

            #[automatically_derived]
            impl #impl_generics #row::RowBatchBuilder<#ident #ty_generics> for #builder_name #ty_generics #where_clause {
                #[inline]
                fn len(&self) -> usize {
                    self.#len
                }

                fn push(&mut self, row: #ident #ty_generics) {
                    #(
                        <<#field_types as #row::RowFormat>::Builder as #row::RowBatchBuilder<#field_types>>::push(&mut self.#builder_fields, row.#field_idents.into());
                    )*
                    self.#len += 1;
                }

                fn build_columns(&mut self) -> #crt::Result<::std::vec::Vec<#crt::derive::ArrayRef>> {
                    // TODO: internal state should be consistent on error
                    let mut cols = ::std::vec::Vec::with_capacity(<#ident #ty_generics as #row::RowFormat>::COLUMNS);
                    #(
                        cols.extend(<<#field_types as #row::RowFormat>::Builder as #row::RowBatchBuilder<#field_types>>::build_columns(&mut self.#builder_fields)?);
                    )*
                    self.#len = 0;
                    Ok(cols)
                }
            }
        }
    }

    fn impl_view(&self) -> TokenStream {
        let crt = &self.crt;
        let row = quote! { #crt::common::row };
        let vis = &self.vis;
        let ident = &self.ident;
        let generics = &self.generics;
        let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

        let view_name = &self.view_name;
        let field_types = self.field_types();
        let field_idents = self.field_idents();
        let view_fields = self
            .field_names()
            .into_iter()
            .map(|name| syn::Ident::new(&name, Span::call_site()))
            .collect::<Vec<_>>();

        let len = syn::Ident::new("_synapse_len", Span::call_site());
        let doc = format!("[`{}::RowFormatView`] for [`{}`]", row, ident);

        let impl_accessors = if self.style.is_tuple() {
            quote! {
                fn row(&self, i: usize) -> #ident #ty_generics {
                    #ident(
                        #(<<#field_types as #row::RowFormat>::View as #row::RowFormatView<#field_types>>::row(&self.#view_fields, i).into(), )*
                    )
                }

                unsafe fn row_unchecked(&self, i: usize) -> #ident #ty_generics {
                    #ident(
                        #(<<#field_types as #row::RowFormat>::View as #row::RowFormatView<#field_types>>::row_unchecked(&self.#view_fields, i).into(), )*
                    )
                }
            }
        } else {
            quote! {
                fn row(&self, i: usize) -> #ident #ty_generics {
                    #ident {
                        #(#field_idents: <<#field_types as #row::RowFormat>::View as #row::RowFormatView<#field_types>>::row(&self.#view_fields, i).into(), )*
                    }
                }

                unsafe fn row_unchecked(&self, i: usize) -> #ident #ty_generics {
                    #ident {
                        #(#field_idents: <<#field_types as #row::RowFormat>::View as #row::RowFormatView<#field_types>>::row_unchecked(&self.#view_fields, i).into(), )*
                    }
                }
            }
        };

        quote! {
            #[doc = #doc]
            #[derive(Debug, Clone)]
            #vis struct #view_name #generics {
                #len: usize,
                #(#view_fields: <#field_types as #row::RowFormat>::View, )*
            }

            #[automatically_derived]
            impl #impl_generics #view_name #ty_generics #where_clause {
                fn new(rows: usize, mut fields: &[::std::sync::Arc<#crt::derive::Field>], mut arrays: &[#crt::derive::ArrayRef]) -> #crt::Result<#view_name #ty_generics> {
                    if arrays.len() != <#ident #ty_generics as #row::RowFormat>::COLUMNS {
                        return Err(#crt::Error::ColumnCount(<#ident as #row::RowFormat>::COLUMNS, fields.len()));
                    }

                    #(
                        let cols = <#field_types as #row::RowFormat>::COLUMNS;
                        let #view_fields = <#field_types as #row::RowFormat>::view(rows, &fields[..cols], &arrays[..cols])?;
                        debug_assert_eq!(<<#field_types as #row::RowFormat>::View as #row::RowFormatView<#field_types>>::len(&#view_fields), rows);
                        fields = &fields[cols..];
                        arrays = &arrays[cols..];
                    )*

                    Ok(#view_name {
                        #len: rows,
                        #(#view_fields, )*
                    })
                }
            }

            #[automatically_derived]
            impl #impl_generics #row::RowFormatView<#ident #ty_generics> for #view_name #ty_generics #where_clause {
                #[inline]
                fn len(&self) -> usize {
                    self.#len
                }

                #impl_accessors
            }

            #[automatically_derived]
            impl #impl_generics ::core::iter::IntoIterator for #view_name #ty_generics #where_clause {
                type Item = #ident #ty_generics;
                type IntoIter = #row::RowViewIter<#ident #ty_generics, #view_name #ty_generics>;

                fn into_iter(self) -> Self::IntoIter {
                    #row::RowViewIter::new(self)
                }
            }
        }
    }

    fn field_idents(&self) -> Vec<TokenStream> {
        self.fields.iter().map(|c| c.ident.clone()).collect()
    }

    fn field_types(&self) -> Vec<syn::Type> {
        self.fields.iter().map(|c| c.ty.clone()).collect()
    }

    fn field_names(&self) -> Vec<String> {
        self.fields.iter().map(|c| c.name.clone()).collect()
    }

    fn with_bounds(mut generics: Generics, crt: &TokenStream) -> Generics {
        for param in &mut generics.params {
            if let GenericParam::Type(ref mut param) = *param {
                param
                    .bounds
                    .push(parse_quote!(#crt::common::row::RowFormat));
                param.bounds.push(parse_quote!(::core::fmt::Debug));
                param.bounds.push(parse_quote!(::core::clone::Clone));
            }
        }
        generics
    }
}

fn synapse_crate() -> TokenStream {
    let crt = proc_macro_crate::crate_name("synapse").expect("synapse crate not found in manifest");
    match crt {
        FoundCrate::Itself => quote! { ::synapse },
        FoundCrate::Name(name) => {
            let ident = format_ident!("{name}");
            quote! { ::#ident }
        }
    }
}

/// Search field attributes for `type` key and replace with raw `r#type`.
///
/// See [TedDriggs/darling#238](https://github.com/TedDriggs/darling/issues/238)
fn fix_field_attrs(input: &mut DeriveInput) -> Result<(), syn::Error> {
    match &mut input.data {
        syn::Data::Struct(data) => {
            for f in &mut data.fields {
                for attr in &mut f.attrs {
                    if attr.path().is_ident("row") {
                        if let syn::Meta::List(list) = &mut attr.meta {
                            list.tokens = std::mem::take(&mut list.tokens)
                                .into_iter()
                                .map(|token| match token {
                                    TokenTree::Ident(ident) if ident == "type" => TokenTree::Ident(
                                        proc_macro2::Ident::new_raw("type", ident.span()),
                                    ),
                                    _ => token,
                                })
                                .collect();
                        }
                    }
                }
            }
        }
        syn::Data::Enum(data) => {
            return Err(syn::Error::new(
                data.enum_token.span,
                "RowFormat macro does not support enums".to_string(),
            ))
        }
        syn::Data::Union(data) => {
            return Err(syn::Error::new(
                data.union_token.span,
                "RowFormat macro does not support unions".to_string(),
            ))
        }
    }
    Ok(())
}
