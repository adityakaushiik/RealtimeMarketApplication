"""Check time boundaries"""
import datetime
import pytz

tz = pytz.timezone('Asia/Kolkata')
now_utc = datetime.datetime.now(datetime.timezone.utc)
now_local = datetime.datetime.now(tz)

print(f'Now (UTC): {now_utc}')
print(f'Now (IST): {now_local}')
print(f'Date (IST): {now_local.date()}')

# Today boundary
today_start_local = now_local.replace(hour=0, minute=0, second=0, microsecond=0)
today_boundary_utc = today_start_local.astimezone(datetime.timezone.utc)
print(f'Today start (IST): {today_start_local}')
print(f'Today boundary (UTC): {today_boundary_utc}')

# NSE open time
nse_open_utc = datetime.datetime(2026, 1, 20, 3, 45, 0, tzinfo=datetime.timezone.utc)
print(f'NSE open today (UTC): {nse_open_utc}')
print(f'NSE open today (IST): {nse_open_utc.astimezone(tz)}')

print()
print(f'Is NSE open >= today_boundary_utc? {nse_open_utc >= today_boundary_utc}')
print(f'So NSE data from today SHOULD be fetched from Redis, not DB')

