default:
    just --list

prerelease version:
    just generate_readme
    just generate_release_notes {{version}}

generate_release_notes version:
    git cliff -p CHANGELOG.md --tag {{version}} -u

generate_readme:
    cd ella; cargo rdme
