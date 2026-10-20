# Vendored browser assets

`markdown-it-15.0.3.min.js` is the `dist/browser/markdown-it.umd.min.js` file from
the public [`markdown-it` 15.0.3 npm package](https://github.com/markdown-it/markdown-it).
The package tarball integrity was checked against npm metadata before extraction:

```text
sha512-+Az7Ojz3iRGu8KYMvJ6xVhvrxP3eE+vEtJX75Ow88XBpcQNV/0lGX5ZynIIBftREo4DC2jsveAMDaeltmm8QvQ==
```

The MIT license is included in `markdown-it.LICENSE`. The app serves this local
copy; it does not fetch renderer code from a CDN at runtime. Markdown parsing is
configured with raw HTML disabled, and rendered markup is shown in a sandboxed
frame with a restrictive content policy.
