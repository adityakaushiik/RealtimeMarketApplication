"""Find dhanhq methods"""
import sys
sys.path.insert(0, r"C:\Users\tech\OneDrive\Documents\GitHub\RealtimeMarketApplication")

from dhanhq import dhanhq
from config.settings import get_settings

settings = get_settings()
dhan = dhanhq(settings.DHAN_CLIENT_ID, settings.DHAN_ACCESS_TOKEN)

output = []
output.append("All dhanhq instance methods:")
for m in dir(dhan):
    if not m.startswith('_') and callable(getattr(dhan, m)):
        output.append(f"  {m}")

# Try to find chart-related methods
chart_methods = [m for m in dir(dhan) if 'chart' in m.lower() or 'ohlc' in m.lower() or 'intraday' in m.lower() or 'historical' in m.lower()]
output.append(f"\nChart-related methods: {chart_methods}")

# Write to file
with open("z_tests/dhanhq_methods_output.txt", "w") as f:
    f.write("\n".join(output))

print("Output written to z_tests/dhanhq_methods_output.txt")

