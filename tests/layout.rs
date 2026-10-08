//! Enforces the layout rules of CONVENTIONS.md that rustfmt does not:
//!
//! - A statement or item that ends with a closing brace on a line of its own
//!   (a block, a closure, a struct literal) is followed by a blank line before
//!   the next statement or item.
//! - Every function, method, struct, enum, trait and impl block whose body
//!   spans several lines ends with a marker that names it: `// end fn name`,
//!   `// end method name`, `// end struct Name`, `// end enum Name`,
//!   `// end trait Name`, `// end impl Name` or `// end impl Trait for Name`.
//!
//! Every Rust file of the crate is checked: sources, tests and examples.

use std::fs;
use std::path::{Path, PathBuf};

use proc_macro2::Span;
use syn::spanned::Spanned;
use syn::visit::{self, Visit};

#[test]
fn every_file_follows_the_layout_rules() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut violations = Vec::new();

    for directory in ["src", "tests", "examples"] {
        for path in rust_files(&root.join(directory)) {
            violations.extend(check_file(root, &path));
        }
    }

    assert!(
        violations.is_empty(),
        "layout violations:\n{}",
        violations.join("\n")
    );
} // end fn every_file_follows_the_layout_rules

fn rust_files(directory: &Path) -> Vec<PathBuf> {
    let Ok(entries) = fs::read_dir(directory) else {
        return Vec::new();
    };

    let mut files = Vec::new();

    for entry in entries.flatten() {
        let path = entry.path();

        if path.is_dir() {
            files.extend(rust_files(&path));
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            files.push(path);
        }
    }

    files
} // end fn rust_files

fn check_file(root: &Path, path: &Path) -> Vec<String> {
    let source = fs::read_to_string(path).expect("source files are readable");
    let syntax = syn::parse_file(&source).expect("source files parse");
    let display = path
        .strip_prefix(root)
        .unwrap_or(path)
        .display()
        .to_string();

    let mut checker = LayoutChecker {
        lines: source.lines().collect(),
        file: display,
        violations: Vec::new(),
    };

    checker.check_items(&syntax.items);
    checker.visit_file(&syntax);

    checker.violations
} // end fn check_file

struct LayoutChecker<'source> {
    lines: Vec<&'source str>,
    file: String,
    violations: Vec<String>,
} // end struct LayoutChecker

impl LayoutChecker<'_> {
    /// Requires a blank line after every element of a sequence of statements
    /// or items that ends with a closing brace on a line of its own.
    ///
    /// A `use` declaration is exempt: grouped imports stay together.
    fn check_sequence(&mut self, elements: impl Iterator<Item = (Span, bool)>) {
        let elements: Vec<(Span, bool)> = elements.collect();

        for pair in elements.windows(2) {
            let ((span, exempt), (next_span, _)) = (pair[0], pair[1]);
            let (start, end) = (span.start().line, span.end().line);
            let next = next_span.start().line;
            let ends_with_brace = self.line(end).trim_start().starts_with('}');

            if exempt || end == start || next <= end || !ends_with_brace {
                continue;
            }

            let blank = (end + 1..next).any(|line| self.line(line).trim().is_empty());

            if !blank {
                self.violations.push(format!(
                    "{}:{}: leave a blank line after the block that ends on line {end}",
                    self.file, next
                ));
            }
        }
    } // end method check_sequence

    fn check_items(&mut self, items: &[syn::Item]) {
        self.check_sequence(
            items
                .iter()
                .map(|item| (item.span(), matches!(item, syn::Item::Use(_)))),
        );
    } // end method check_items

    /// Requires `// end <kind> <name>` on the line that closes a multi-line
    /// body.
    fn check_marker(&mut self, span: Span, kind: &str, name: &str) {
        if span.start().line == span.end().line {
            return;
        }

        let line = span.end().line;
        let expected = format!("// end {kind} {name}");

        if !self.line(line).trim_end().ends_with(&expected) {
            self.violations.push(format!(
                "{}:{line}: end the block with `{expected}`",
                self.file
            ));
        }
    } // end method check_marker

    fn line(&self, number: usize) -> &str {
        self.lines.get(number - 1).copied().unwrap_or("")
    } // end method line
} // end impl LayoutChecker

impl<'syntax> Visit<'syntax> for LayoutChecker<'_> {
    fn visit_block(&mut self, block: &'syntax syn::Block) {
        self.check_sequence(block.stmts.iter().map(|statement| {
            let exempt = matches!(statement, syn::Stmt::Item(syn::Item::Use(_)));

            (statement.span(), exempt)
        }));

        visit::visit_block(self, block);
    } // end method visit_block

    fn visit_item_fn(&mut self, function: &'syntax syn::ItemFn) {
        self.check_marker(function.block.span(), "fn", &function.sig.ident.to_string());
        visit::visit_item_fn(self, function);
    } // end method visit_item_fn

    fn visit_impl_item_fn(&mut self, function: &'syntax syn::ImplItemFn) {
        let kind = if function.sig.receiver().is_some() {
            "method"
        } else {
            "fn"
        };

        self.check_marker(function.block.span(), kind, &function.sig.ident.to_string());
        visit::visit_impl_item_fn(self, function);
    } // end method visit_impl_item_fn

    fn visit_trait_item_fn(&mut self, function: &'syntax syn::TraitItemFn) {
        if let Some(block) = &function.default {
            let kind = if function.sig.receiver().is_some() {
                "method"
            } else {
                "fn"
            };

            self.check_marker(block.span(), kind, &function.sig.ident.to_string());
        }

        visit::visit_trait_item_fn(self, function);
    } // end method visit_trait_item_fn

    fn visit_item_struct(&mut self, item: &'syntax syn::ItemStruct) {
        if let syn::Fields::Named(fields) = &item.fields {
            self.check_marker(fields.span(), "struct", &item.ident.to_string());
        }

        visit::visit_item_struct(self, item);
    } // end method visit_item_struct

    fn visit_item_enum(&mut self, item: &'syntax syn::ItemEnum) {
        self.check_marker(
            item.brace_token.span.join(),
            "enum",
            &item.ident.to_string(),
        );
        visit::visit_item_enum(self, item);
    } // end method visit_item_enum

    fn visit_item_trait(&mut self, item: &'syntax syn::ItemTrait) {
        self.check_marker(
            item.brace_token.span.join(),
            "trait",
            &item.ident.to_string(),
        );
        self.check_sequence(item.items.iter().map(|item| (item.span(), false)));
        visit::visit_item_trait(self, item);
    } // end method visit_item_trait

    fn visit_item_impl(&mut self, item: &'syntax syn::ItemImpl) {
        let type_name = last_segment(&item.self_ty);

        let name = match &item.trait_ {
            Some((_, path, _)) => {
                let trait_name = path
                    .segments
                    .last()
                    .map(|segment| segment.ident.to_string());

                format!("{} for {type_name}", trait_name.unwrap_or_default())
            }
            None => type_name,
        };

        self.check_marker(item.brace_token.span.join(), "impl", &name);
        self.check_sequence(item.items.iter().map(|item| (item.span(), false)));
        visit::visit_item_impl(self, item);
    } // end method visit_item_impl

    fn visit_item_mod(&mut self, item: &'syntax syn::ItemMod) {
        if let Some((_, items)) = &item.content {
            self.check_items(items);
        }

        visit::visit_item_mod(self, item);
    } // end method visit_item_mod
} // end impl Visit for LayoutChecker

/// The type's own name, without its path or generics.
fn last_segment(path_type: &syn::Type) -> String {
    match path_type {
        syn::Type::Path(type_path) => type_path
            .path
            .segments
            .last()
            .map(|segment| segment.ident.to_string())
            .unwrap_or_default(),
        _ => String::new(),
    }
} // end fn last_segment
