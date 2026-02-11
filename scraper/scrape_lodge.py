import json
import re
import time
import os
import sys
from datetime import datetime, timezone

try:
    import cloudscraper
except ImportError:
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "cloudscraper"])
    import cloudscraper


# ============================================
# CONFIG
# ============================================

HARDCODED_TRAINERS = [
    "Acerola", "Adaman", "Arven", "Ball Guy", "Blue", "Brendan",
    "Calem", "Carmine", "Cheren", "Cynthia", "Dawn", "Diantha",
    "Elesa", "Giovanni", "Gladion", "Gloria", "Grimsley", "Hilda",
    "Iono", "Irida", "Iris", "Jasmine", "Kabu", "Lacey", "Lana",
    "Lance", "Larry", "Leaf", "Lear", "Leon", "Lillie", "Lusamine",
    "Marnie", "May", "Morty", "N", "Penny", "Piers",
    "Professor Sycamore", "Raihan", "Rika", "Rosa", "Serena",
    "Shauna", "Silver", "Skyla", "Steven", "Volkner", "Volo", "Wally"
]

SKIP_PAGES = {
    "Trainer Lodge", "Trainer Lodge/Expeditions",
    "Trainer Lodge/Lodge Exchange", "Trainer Lodge/Redecorate"
}

API_URL = "https://pokemon-masters-ex-game.fandom.com/api.php"

# Paths - works both locally and in GitHub Actions
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_DIR = os.path.dirname(SCRIPT_DIR)
DATA_DIR = os.path.join(REPO_DIR, "data")
DATA_FILE = os.path.join(DATA_DIR, "trainer_lodge_data.json")
CHANGELOG_FILE = os.path.join(DATA_DIR, "changelog.json")

MAX_RETRIES = 3
RETRY_DELAY = 5


# ============================================
# HTTP REQUEST WITH RETRY
# ============================================

def api_request(scraper, params, retries=MAX_RETRIES):
    for attempt in range(retries):
        try:
            resp = scraper.get(API_URL, params=params, timeout=30)

            if resp.status_code != 200:
                print("HTTP {} (attempt {}/{})".format(
                    resp.status_code, attempt + 1, retries))
                if attempt < retries - 1:
                    time.sleep(RETRY_DELAY * (attempt + 1))
                continue

            # Check if we got the Cloudflare challenge instead of JSON
            content_type = resp.headers.get("content-type", "")
            if "json" not in content_type:
                print("Got non-JSON response (attempt {}/{})".format(
                    attempt + 1, retries))
                if attempt < retries - 1:
                    # Create new scraper instance to get fresh cookies
                    scraper = cloudscraper.create_scraper(
                        browser={"browser": "chrome", "platform": "windows", "mobile": False}
                    )
                    time.sleep(RETRY_DELAY * (attempt + 1))
                continue

            data = resp.json()
            return data, scraper

        except Exception as e:
            print("Request error: {} (attempt {}/{})".format(
                str(e)[:50], attempt + 1, retries))
            if attempt < retries - 1:
                time.sleep(RETRY_DELAY * (attempt + 1))

    return None, scraper


# ============================================
# WIKITEXT PARSER
# ============================================

def parse_wikitext(wikitext, trainer_name):
    result = {
        "name": trainer_name,
        "tiers": {}
    }

    tier_pattern = re.compile(
        r"===\s*(Interesting|Exciting|Super Exciting)\s*==="
    )
    tier_matches = list(tier_pattern.finditer(wikitext))

    for i, match in enumerate(tier_matches):
        tier_name = match.group(1)
        start = match.end()

        if i + 1 < len(tier_matches):
            end = tier_matches[i + 1].start()
        else:
            scrapbook = re.search(r"==\s*Scrapbook", wikitext[start:])
            end = start + scrapbook.start() if scrapbook else len(wikitext)

        tier_content = wikitext[start:end]
        categories = parse_tier_content(tier_content)

        if categories:
            result["tiers"][tier_name] = categories

    return result


def parse_tier_content(content):
    categories = {}
    current_category = None

    for line in content.split("\n"):
        line = line.strip()

        if not line or line == "|-" or line == "|}" or line.startswith("{|"):
            continue

        # Category header: !colspan="X"|CategoryName
        cat_match = re.match(r'!colspan=["\']?\d+["\']?\|(.+)', line)
        if cat_match:
            current_category = cat_match.group(1).strip()
            # Normalize Pokemon variants
            if "Pok" in current_category:
                current_category = "Pokemon"
            categories[current_category] = []
            continue

        # Topic line: |TopicName
        if current_category and line.startswith("|") and not line.startswith("{|"):
            raw = line[1:].strip()

            # Skip table formatting
            if not raw or raw.startswith("class=") or raw.startswith("style="):
                continue

            # Handle multiple topics: |Topic1||Topic2
            if "||" in raw:
                for t in raw.split("||"):
                    t = t.strip()
                    if t:
                        categories[current_category].append(t)
            else:
                categories[current_category].append(raw)

    return categories


# ============================================
# DISCOVER NEW TRAINERS FROM WIKI CATEGORY
# ============================================

def discover_trainers(scraper):
    print("\nDiscovering trainers from wiki category page...")

    params = {
        "action": "query",
        "list": "categorymembers",
        "cmtitle": "Category:Trainer_Lodge",
        "cmlimit": "500",
        "format": "json"
    }

    data, scraper = api_request(scraper, params)

    if not data:
        print("   Failed to fetch category page")
        return [], scraper

    members = data.get("query", {}).get("categorymembers", [])
    wiki_trainers = []

    for member in members:
        title = member.get("title", "")
        if title.startswith("Trainer Lodge/") and title not in SKIP_PAGES:
            name = title.replace("Trainer Lodge/", "")
            # Extra safety: skip if name looks wrong
            if len(name) > 0 and len(name) < 50:
                wiki_trainers.append(name)

    print("   Found {} trainer pages on wiki".format(len(wiki_trainers)))
    return wiki_trainers, scraper


# ============================================
# LOAD EXISTING DATA
# ============================================

def load_existing_data():
    if not os.path.exists(DATA_FILE):
        return {}
    try:
        with open(DATA_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        # Remove metadata key
        return {k: v for k, v in data.items() if k != "_metadata"}
    except Exception as e:
        print("Warning: Could not load existing data: {}".format(e))
        return {}


def load_changelog():
    if not os.path.exists(CHANGELOG_FILE):
        return {"updates": []}
    try:
        with open(CHANGELOG_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"updates": []}


# ============================================
# COMPARE DATA FOR CHANGES
# ============================================

def compare_data(old_data, new_data):
    changes = []

    old_names = set(old_data.keys())
    new_names = set(new_data.keys())

    # New trainers
    for name in sorted(new_names - old_names):
        tiers = new_data[name].get("tiers", {})
        topic_count = sum(
            sum(len(items) for items in cats.values())
            for cats in tiers.values()
        )
        changes.append({
            "type": "new_trainer",
            "trainer": name,
            "details": "{} tiers, {} topics".format(len(tiers), topic_count)
        })

    # Removed trainers
    for name in sorted(old_names - new_names):
        changes.append({
            "type": "removed_trainer",
            "trainer": name
        })

    # Modified trainers
    for name in sorted(old_names & new_names):
        old_tiers = old_data[name].get("tiers", {})
        new_tiers = new_data[name].get("tiers", {})

        if json.dumps(old_tiers, sort_keys=True) == json.dumps(new_tiers, sort_keys=True):
            continue

        details = []
        all_tier_names = sorted(set(list(old_tiers.keys()) + list(new_tiers.keys())))

        for tier_name in all_tier_names:
            old_cats = old_tiers.get(tier_name, {})
            new_cats = new_tiers.get(tier_name, {})
            all_cat_names = sorted(set(list(old_cats.keys()) + list(new_cats.keys())))

            for cat_name in all_cat_names:
                old_topics = set(old_cats.get(cat_name, []))
                new_topics = set(new_cats.get(cat_name, []))

                added = sorted(new_topics - old_topics)
                removed = sorted(old_topics - new_topics)

                if added:
                    details.append("+[{}/{}]: {}".format(
                        tier_name, cat_name, ", ".join(added)))
                if removed:
                    details.append("-[{}/{}]: {}".format(
                        tier_name, cat_name, ", ".join(removed)))

        if details:
            changes.append({
                "type": "modified_trainer",
                "trainer": name,
                "details": "; ".join(details)
            })

    return changes


# ============================================
# SAVE DATA
# ============================================

def save_data(new_data, changes, changelog, errors):
    os.makedirs(DATA_DIR, exist_ok=True)

    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    # Calculate total topics
    total_topics = 0
    for trainer in new_data.values():
        for cats in trainer.get("tiers", {}).values():
            for items in cats.values():
                total_topics += len(items)

    # Build output with metadata first
    output = {
        "_metadata": {
            "last_updated": now,
            "trainer_count": len(new_data),
            "total_topics": total_topics,
            "source": "pokemon-masters-ex-game.fandom.com",
            "scraper_version": "2.0",
            "errors": errors if errors else []
        }
    }

    # Add trainers sorted alphabetically
    for name in sorted(new_data.keys()):
        output[name] = new_data[name]

    # Save main data
    with open(DATA_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
    print("Saved: {} ({} trainers, {} topics)".format(
        DATA_FILE, len(new_data), total_topics))

    # Update changelog
    if changes:
        changelog["updates"].insert(0, {
            "date": now,
            "trainer_count": len(new_data),
            "change_count": len(changes),
            "changes": changes
        })
        # Keep last 50 entries
        changelog["updates"] = changelog["updates"][:50]

    with open(CHANGELOG_FILE, "w", encoding="utf-8") as f:
        json.dump(changelog, f, indent=2, ensure_ascii=False)
    print("Saved: {}".format(CHANGELOG_FILE))


# ============================================
# MAIN
# ============================================

def main():
    print("=" * 60)
    print("POKEMON MASTERS EX - TRAINER LODGE SCRAPER v2.0")
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    print("Time: {}".format(now))
    print("=" * 60)

    # Load existing
    existing_data = load_existing_data()
    changelog = load_changelog()
    print("Existing data: {} trainers".format(len(existing_data)))

    # Create scraper
    scraper = cloudscraper.create_scraper(
        browser={"browser": "chrome", "platform": "windows", "mobile": False}
    )

    # Discover trainers from wiki
    wiki_trainers, scraper = discover_trainers(scraper)

    # Merge hardcoded + wiki discovered
    all_trainers = sorted(set(HARDCODED_TRAINERS + wiki_trainers))

    # Remove known non-trainer pages
    skip_names = {"Expeditions", "Lodge Exchange", "Redecorate"}
    all_trainers = [t for t in all_trainers if t not in skip_names]

    new_from_wiki = set(wiki_trainers) - set(HARDCODED_TRAINERS) - skip_names
    if new_from_wiki:
        print("\n*** NEW TRAINERS DISCOVERED: {} ***".format(
            ", ".join(sorted(new_from_wiki))))

    print("\nScraping {} trainers...\n".format(len(all_trainers)))

    # Scrape
    new_data = {}
    errors = []
    total = len(all_trainers)

    for i, name in enumerate(all_trainers):
        pct = ((i + 1) / total) * 100
        print("[{}/{}] ({:.0f}%) {}...".format(
            i + 1, total, pct, name), end=" ", flush=True)

        params = {
            "action": "parse",
            "page": "Trainer Lodge/{}".format(name),
            "format": "json",
            "prop": "wikitext"
        }

        data, scraper = api_request(scraper, params)

        if not data:
            print("FAILED (no response)")
            errors.append(name)
            continue

        if "error" in data:
            error_msg = data["error"].get("info", "unknown")
            print("API error: {}".format(error_msg))
            errors.append(name)
            continue

        try:
            wikitext = data["parse"]["wikitext"]["*"]
        except (KeyError, TypeError):
            print("FAILED (bad response structure)")
            errors.append(name)
            continue

        trainer_result = parse_wikitext(wikitext, name)

        tier_count = len(trainer_result["tiers"])
        topic_count = sum(
            sum(len(items) for items in cats.values())
            for cats in trainer_result["tiers"].values()
        )

        if tier_count > 0:
            new_data[name] = trainer_result
            print("OK - {} tiers, {} topics".format(tier_count, topic_count))
        else:
            print("EMPTY (0 tiers parsed)")
            errors.append(name)

        time.sleep(0.5)

    # Compare
    print("\n" + "=" * 60)
    print("COMPARING DATA...")
    print("=" * 60)

    changes = compare_data(existing_data, new_data)

    if changes:
        print("\n{} changes detected:".format(len(changes)))
        for c in changes:
            ctype = c["type"].upper().replace("_", " ")
            detail = c.get("details", "")
            print("   [{}] {}{}".format(
                ctype, c["trainer"],
                " - " + detail[:120] if detail else ""))
    else:
        print("\nNo changes detected.")

    # Save
    print("\n" + "=" * 60)
    print("SAVING...")
    print("=" * 60)

    save_data(new_data, changes, changelog, errors)

    # Final summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print("   Trainers scraped: {}/{}".format(len(new_data), total))
    print("   Errors: {}".format(len(errors)))
    if errors:
        print("   Failed: {}".format(", ".join(errors)))
    print("   Changes: {}".format(len(changes)))

    # Fail if more than half failed
    if len(errors) > total * 0.5:
        print("\nERROR: Too many failures!")
        sys.exit(1)

    print("\nDone!")


if __name__ == "__main__":
    main()
