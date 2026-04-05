"""Bulk-load RDF files from a directory into a GraphDB repository via REST API."""

import argparse
import os
import sys
import requests

MIME_TYPES = {
    ".ttl": "text/turtle",
    ".nt": "application/n-triples",
    ".rdf": "application/rdf+xml",
    ".owl": "application/rdf+xml",
    ".jsonld": "application/ld+json",
}


def validate_repo(host, repo):
    """Check that the repository exists on the GraphDB server."""
    url = f"{host}/repositories"
    try:
        resp = requests.get(url, headers={"Accept": "application/json"}, timeout=10)
        resp.raise_for_status()
    except requests.ConnectionError:
        print(f"ERROR: Cannot connect to GraphDB at {host}")
        sys.exit(1)
    except requests.HTTPError as e:
        print(f"ERROR: Failed to list repositories: {e}")
        sys.exit(1)

    repos = [r.get("id", "") for r in resp.json().get("results", {}).get("bindings", [])]
    # GraphDB may return repos in SPARQL-result format or plain JSON depending on version
    if not repos:
        # Try plain list format
        repos = [r.get("id", "") for r in resp.json()] if isinstance(resp.json(), list) else []

    if repo not in repos:
        print(f"WARNING: Repository '{repo}' not found in {repos}. Proceeding anyway (it may still accept data).")


def clear_repo(host, repo):
    """Delete all statements from the repository."""
    url = f"{host}/repositories/{repo}/statements"
    print(f"Clearing repository '{repo}'...")
    resp = requests.delete(url, timeout=60)
    if resp.ok:
        print(f"Repository '{repo}' cleared.")
    else:
        print(f"ERROR clearing repository: {resp.status_code} {resp.text}")
        sys.exit(1)


def load_file(host, repo, filepath):
    """Load a single RDF file into the repository."""
    ext = os.path.splitext(filepath)[1].lower()
    content_type = MIME_TYPES.get(ext)
    if not content_type:
        print(f"  SKIP {filepath} (unsupported extension '{ext}')")
        return False

    url = f"{host}/repositories/{repo}/statements"
    filename = os.path.basename(filepath)

    with open(filepath, "r", encoding="utf-8") as f:
        data = f.read()

    if not data.strip():
        print(f"  SKIP {filename} (empty file)")
        return False

    resp = requests.post(
        url,
        data=data.encode("utf-8"),
        headers={"Content-Type": content_type},
        timeout=300,
    )

    if resp.ok:
        print(f"  OK   {filename}")
        return True
    else:
        print(f"  FAIL {filename} — {resp.status_code}: {resp.text[:200]}")
        return False


def collect_files(directory):
    """Collect all supported RDF files from a directory (non-recursive)."""
    files = []
    for name in sorted(os.listdir(directory)):
        ext = os.path.splitext(name)[1].lower()
        if ext in MIME_TYPES:
            files.append(os.path.join(directory, name))
    return files


def main():
    parser = argparse.ArgumentParser(description="Load RDF files into GraphDB")
    parser.add_argument("--dir", default="../data/rdf/",
                        help="Directory containing RDF files (default: ../data/rdf/)")
    parser.add_argument("--repo", default="sakunagraph",
                        help="GraphDB repository name (default: sakunagraph)")
    parser.add_argument("--host", default="http://localhost:7200",
                        help="GraphDB server URL (default: http://localhost:7200)")
    parser.add_argument("--clear", action="store_true",
                        help="Clear the repository before loading")
    parser.add_argument("--include-ontology", action="store_true",
                        help="Also load ontology files from ontology/ directory")
    args = parser.parse_args()

    validate_repo(args.host, args.repo)

    if args.clear:
        clear_repo(args.host, args.repo)

    files = collect_files(args.dir)

    if args.include_ontology:
        ontology_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "ontology")
        if os.path.isdir(ontology_dir):
            ontology_files = collect_files(ontology_dir)
            print(f"\nLoading {len(ontology_files)} ontology file(s) from {ontology_dir}")
            files = ontology_files + files
        else:
            print(f"WARNING: Ontology directory not found at {ontology_dir}")

    if not files:
        print(f"No RDF files found in {args.dir}")
        sys.exit(0)

    print(f"\nLoading {len(files)} file(s) into {args.host}/repositories/{args.repo}\n")

    success = 0
    failed = 0
    for filepath in files:
        if load_file(args.host, args.repo, filepath):
            success += 1
        else:
            failed += 1

    print(f"\nDone: {success} loaded, {failed} failed/skipped out of {len(files)} files.")


if __name__ == "__main__":
    main()
