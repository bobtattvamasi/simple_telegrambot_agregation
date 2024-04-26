from pyrogram import Client, filters
import json
import logging
import pymongo
import asyncio

from config.config import Config, load_config
from utils import find_values_by_date_aggregation, calculate_salary


logger = logging.getLogger(__name__)

logging.basicConfig(
        level=logging.INFO,
        format="%(filename)s:%(lineno)d #%(levelname)-8s "
               "[%(asctime)s] - %(name)s - %(message)s",
    )

config: Config = load_config()

logger.info(config.tg_bot.token)


app = Client("RLT_bogdan_test_task_bot", api_hash='cdc076e98d896e6eac285366847102e8', api_id=22897851, bot_token=config.tg_bot.token)

@app.on_message(filters.private)
async def handle_message(client, message):
    try:
        data = json.loads(message.text)
        # Extract date range and group type
        dt_from = data.get("dt_from")
        dt_upto = data.get("dt_upto")
        group_type = data.get("group_type")

        # Validate data
        if not all([dt_from, dt_upto, group_type]):
            raise ValueError("Missing required fields in JSON (dt_from, dt_upto, group_type)")

        # Connect to MongoDB (replace with your connection logic)
        client = pymongo.MongoClient("mongodb://localhost:27017/")
        db = client["sampleDB"]
        collection = db["sample_collection"]


        documents = find_values_by_date_aggregation(collection, dt_from, dt_upto)

        response_data = calculate_salary(documents, group_type)

        # Respond to the user with the aggregated data
        await message.reply_text(json.dumps(response_data))

    except (ValueError, json.JSONDecodeError) as e:
        logger.error(f"Error processing message: {e}")
        await message.reply_text(f"Invalid JSON message or missing data: {e}")

if __name__ == "__main__":
    try:
        logger.info("Starting bot")
        app.run()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Bot stopped")