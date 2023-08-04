default:
    just --list

prerelease version:
    just generate_readme
    just generate_release_notes {{version}}

generate_release_notes version:
    git cliff -p CHANGELOG.md --tag {{version}} -u

generate_readme:
    cd ella; cargo rdme

generate_python:
    cargo run -p generate_typing --bin generate_data_types
