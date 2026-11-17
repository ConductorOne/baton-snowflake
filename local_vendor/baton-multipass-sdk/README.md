# baton-multipass-sdk (temporary local copy)

Temporary, trimmed copy of `github.com/conductorone/baton-multipass-sdk`
(`pkg/multipass`) at v0.0.1. This copy exists until that module is published.

It is an ordinary package of this module, imported as
`github.com/conductorone/baton-snowflake/local_vendor/baton-multipass-sdk/pkg/multipass`.
There is no nested `go.mod`, `require` or `replace`.

Do not edit this code to diverge from upstream.

To switch to the published module: add it to `go.mod`, change the import path in
`pkg/connector`, delete this folder, then run `go mod tidy && go mod vendor`.
