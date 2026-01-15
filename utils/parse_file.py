async def parse_csv_file(file_path: str) -> list[dict[str, str]]:
    """Parse a CSV file and return a list of dictionaries representing rows."""
    import csv

    data = []
    with open(file_path, mode="r", newline="", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            data.append(row)
            # print(row)
    return data


async def parse_excel_file(file_path: str) -> list[dict[str, str]]:
    """Parse an Excel file and return a list of dictionaries representing rows."""
    import pandas as pd

    df = pd.read_excel(file_path)
    return df.to_dict(orient="records")


# data_from_csv = await parse_csv_file("C:/Users/tech/Downloads/ind_nifty500list.csv")
# async for session in get_db_session():
#
#     existing_instruments = await session.execute(select(Instrument).distinct())
#     existing_instruments = existing_instruments.scalars().all()
#     print("Existing instruments:", len(existing_instruments))
#
#     existing_instruments_symbol = [inst.symbol for inst in existing_instruments]
#     gather_tasks = []
#     for row in data_from_csv:
#         if row['Symbol'] in existing_instruments_symbol:
#             continue  # Skip existing instruments
#
#
#         print("Adding instrument:", row['Symbol'], row['Company Name'])
#         exchange_id = await exchange_service.get_exchange_by_code(session=session,code='NSE')
#         exchange_id = exchange_id.id
#         instrument_type_id = 1
#         instrument_create = InstrumentCreate(
#             name=row['Company Name'],
#             symbol=row['Symbol'],
#             exchange_id=exchange_id,
#             instrument_type_id=instrument_type_id,
#             blacklisted=False,
#             delisted=False
#         )
#         await instrument_service.create_instrument(
#             session=session,
#             instrument_data=instrument_create
#         )
# logger.info("Database population from CSV completed.")


# data_from_csv = await parse_csv_file("C:/Users/tech/Downloads/api-scrip-master.csv")
# async for session in get_db_session():
#     existing_instruments = await session.execute(select(Instrument).distinct())
#     existing_instruments = existing_instruments.scalars().all()
#     print("Existing instruments:", len(existing_instruments))
#
#     existing_instruments_symbol_id_map = {inst.symbol: inst.id for inst in existing_instruments}
#     dhan_provider_id = 2
#
#     bulk_add = []
#     for row in data_from_csv:
#         symbol = row["SEM_TRADING_SYMBOL"]
#         security_id = row["SEM_SMST_SECURITY_ID"]
#
#         if symbol in existing_instruments_symbol_id_map:
#             print("Mapping provider instrument for:", symbol, security_id)
#             bulk_add.append(
#                 ProviderInstrumentMapping(
#                     provider_id=dhan_provider_id,
#                     instrument_id=existing_instruments_symbol_id_map[symbol],
#                     provider_instrument_search_code=str(security_id),
#                     is_active=False
#                 )
#             )
#
#     if bulk_add:
#         session.add_all(bulk_add)
#         await session.commit()
#         print(f"Added {len(bulk_add)} provider-instrument mappings.")
#     else:
#         print("No new provider-instrument mappings to add.")


# data_from_csv = await parse_csv_file("C:/Users/tech/Downloads/api-scrip-master.csv")
# async for session in get_db_session():
#
#
#     # FETCH EXISTING INSTRUMENTS
#     existing_instruments = await session.execute(select(Instrument).distinct())
#     existing_instruments = existing_instruments.scalars().all()
#     print("Existing instruments:", len(existing_instruments))
#     existing_instruments_symbol_id_map = {inst.symbol: inst.id for inst in existing_instruments}
#     dhan_provider_id = 2
#
#
#     # Add New Symbols to Instrument Table
#     exchange_id = 7  # NSE Exchange
#     new_instruments = []
#     for row in data_from_csv:
#         instrument_type_id = 1
#         name = row['SM_SYMBOL_NAME']
#         symbol = row['SEM_TRADING_SYMBOL']
#
#         if symbol in existing_instruments_symbol_id_map:
#             continue  # Skip existing instruments
#
#         # name to title case
#         name = name.title()
#
#         instrument_create = Instrument(
#             name=name,
#             symbol=symbol,
#             exchange_id=exchange_id,
#             instrument_type_id=instrument_type_id,
#             blacklisted=False,
#             delisted=False,
#             is_active=True,
#             should_record_data=False
#         )
#         new_instruments.append(instrument_create)
#         print("Adding instrument:", symbol, name)
#     if new_instruments:
#         session.add_all(new_instruments)
#         await session.commit()
#         print(f"Added {len(new_instruments)} new instruments.")
#     else:
#         print("No new instruments to add.")
#
#
#     # Refresh existing instruments after adding new ones
#     existing_instruments = await session.execute(select(Instrument).distinct())
#     existing_instruments = existing_instruments.scalars().all()
#     existing_instruments_symbol_id_map = {inst.symbol: inst.id for inst in existing_instruments}
#
#
#     # Get Provider Instrument Mappings for Dhan
#     existing_mappings = await session.execute(
#         select(
#             ProviderInstrumentMapping
#         ).where(
#             ProviderInstrumentMapping.provider_id == dhan_provider_id
#         )
#     )
#     existing_mappings = existing_mappings.scalars().all()
#     existing_mapped_instrument_ids = {mapping.instrument_id for mapping in existing_mappings}
#
#
#     bulk_add = []
#     for row in data_from_csv:
#         symbol = row["SEM_TRADING_SYMBOL"]
#         security_id = row["SEM_SMST_SECURITY_ID"]
#
#         if symbol in existing_instruments_symbol_id_map and existing_instruments_symbol_id_map[symbol] not in existing_mapped_instrument_ids:
#             print("Mapping provider instrument for:", symbol, security_id)
#             bulk_add.append(
#                 ProviderInstrumentMapping(
#                     provider_id=dhan_provider_id,
#                     instrument_id=existing_instruments_symbol_id_map[symbol],
#                     provider_instrument_search_code=str(security_id),
#                     is_active=True
#                 )
#             )
#
#     if bulk_add:
#         session.add_all(bulk_add)
#         await session.commit()
#         print(f"Added {len(bulk_add)} provider-instrument mappings.")
#     else:
#         print("No new provider-instrument mappings to add.")
