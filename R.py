import subprocess
import json

rscript = "C:/Program Files/R/R-4.5.1/bin/Rscript.exe"
script_path = "C:/Users/David/OneDrive - Bildungsdirektion/Documents/player prop bets/bet_resolver.R"

cmd = [
    rscript,
    script_path,
    "Jeremy Ruckert",
    "nfl",
    "2023",
    "player last touchdown scorer",
    "Miami Dolphins @ New York Jets",
    "NULL",
    "2023-11-24"
]

print("Running command:", " ".join(cmd))
print("-" * 50)

result = subprocess.run(cmd, capture_output=True, text=True, encoding='utf-8')

print("STDOUT length:", len(result.stdout))
print("STDERR length:", len(result.stderr))

# The JSON is in STDERR, not STDOUT!
# Extract just the JSON part from stderr
stderr_lines = result.stderr.strip().split('\n')

# Find the JSON line (it should be the last line)
json_line = None
for line in reversed(stderr_lines):
    line = line.strip()
    if line.startswith('{') and line.endswith('}'):
        json_line = line
        break

if json_line:
    print("\n✅ Found JSON in STDERR!")
    try:
        data = json.loads(json_line)
        print("Parsed JSON successfully!")
        print(json.dumps(data, indent=2))

        # Check the result
        if data.get('success'):
            print(f"\n🎯 Bet resolved: {data.get('resolved', False)}")
            print(f"📊 Player scored last: {data.get('player_scored', False)}")
            print(f"📅 Game ID: {data.get('game_id', 'N/A')}")
            print(f"📅 Game Date: {data.get('game_date', 'N/A')}")
        else:
            print(f"\n❌ Error: {data.get('error', 'Unknown error')}")

    except json.JSONDecodeError as e:
        print(f"\n❌ Failed to parse JSON: {e}")
        print("JSON line:", json_line)
else:
    print("\n❌ No JSON found in output")
    print("Last 5 lines of STDERR:")
    for line in stderr_lines[-5:]:
        print("  ", line)