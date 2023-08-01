# Contributing to ella

## Commits and Pull Requests

Commits to the main branch should follow the [conventional commits](www.conventionalcommits.org) specification.

Commits in other branches don't have to follow the specification but doing so may help speed up PR merging.

Pull requests should be squash merged, and the squashed commit should follow the conventional commits specification.

## Deploying a release

Releases should be generated using [`cargo release`](https://github.com/crate-ci/cargo-release):

```shell
cargo release --workspace --execute <patch/minor/major>
```

This should:

- Increment the version in all crates, dependency specifications, and documentation
- Re-generate crate readme(s)
- Publish releases for all crates to crates.io
- Update the changelog
- Create a tag for the new version

Once the new tag is pushed to the repo, a new Github release and Docker image will be generated automatically by Github Actions.
