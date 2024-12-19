import sys
print(sys.version)

import mylibs.constants as constants
from mylibs.ngsi_ld import geoquery_ngsi_point, retrieve_ngsi_type
import time
# from mylibs.onemap import search_one_map_location
from landtransportsg import PublicTransport

import json
from requests.exceptions import RequestException, HTTPError
from ngsildclient import Client, Entity, SmartDataModels
from datetime import datetime

import logging
from geopy.distance import geodesic
from telegram import Update, ForceReply, ReplyKeyboardMarkup, ReplyKeyboardRemove, InlineKeyboardButton, InlineKeyboardMarkup, InputMediaPhoto
from telegram.ext import ApplicationBuilder, CallbackQueryHandler, CommandHandler, ContextTypes, ConversationHandler, MessageHandler, filters
from telegram.error import BadRequest

import mylibs.google_maps as google_maps
import asyncio

import colorama
from colorama import Fore, Back, Style
colorama.init(autoreset=True)

# Logging setup
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

def ngsi_test_fn():
    ret = geoquery_ngsi_point(input_type="Carpark", maxDistance=5000, lat=103.83359, long=1.3071)
    print(len(ret))

# State definitions
DESTINATION, CONFIRM_DESTINATION, LIVE_LOCATION, RESTART_SESSION, USER_PREFERENCE = range(5)

# Store user data
user_data = {}

# Timeout duration for inactive sessions
TIMEOUT_DURATION = 300

async def timeout(context: ContextTypes.DEFAULT_TYPE):
    """Notify the user that the session has timed out and end the conversation."""
    job = context.job
    chat_id = job.data['chat_id']
    user_data = job.data['user_data']
    start_message_id = user_data.get('start_message_id')
    destination_message_id = user_data.get('destination_message_id')

    if start_message_id:
        try:
            await context.bot.delete_message(
                chat_id=chat_id,
                message_id=start_message_id
            )
        except Exception as e:
            logger.error(f"Failed to delete start message: {e}")

    destination_message_id = user_data.get('destination_message_id')
    if destination_message_id:
        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=destination_message_id)
        except Exception as e:
            logger.error(f"Error deleting the destination message: {e}")

    await context.bot.send_message(
        chat_id=chat_id,
        text="⏳ *Session timed out.* Please start again if you'd like to continue.",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔁 Start Session",callback_data="start",
        )]]),
        parse_mode="Markdown"
    )

    user_data.clear()

async def reset_timeout(context: ContextTypes.DEFAULT_TYPE):
    """Reset the timeout duration for the user's session."""
    chat_id = context.user_data['chat_id']

    if 'timeout_job' in context.user_data:
        old_job = context.user_data['timeout_job']
        old_job.schedule_removal()

    new_job = context.job_queue.run_once(timeout, TIMEOUT_DURATION, data={'chat_id': chat_id, 'user_data': context.user_data})
    context.user_data['timeout_job'] = new_job

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Send a welcome message and ask for user's destination."""
    chat_id = update.message.chat_id if update.message else update.callback_query.message.chat_id
    keyboard = [[InlineKeyboardButton("🛑 End Session", callback_data="end")]]
    reply_markup = InlineKeyboardMarkup(keyboard)

    if update.message:
        sent_message = await update.message.reply_text(
            "👋 *Welcome!* Where would you like to go today?\n\n"
            "Please type your destination.",
            parse_mode='Markdown',
            reply_markup=reply_markup
            )
        context.user_data['start_message_id'] = sent_message.message_id
    elif update.callback_query:
        query = update.callback_query
        await query.answer()
        sent_message = await query.message.edit_text(
            "👋 *Welcome!* Where would you like to go today?\n\n"
            "Please type your destination.",
            parse_mode='Markdown',
            reply_markup=reply_markup
        )
        context.user_data['start_message_id'] = sent_message.message_id

    context.user_data['start_message_edited_status'] = False
    context.user_data['chat_id'] = chat_id

    await reset_timeout(context)

    return DESTINATION

async def get_destination(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle destination input and return a list of suggestions"""
    await reset_timeout(context)
    context.job_queue.stop()
    if context.user_data.get("start_message_edited_status") == False:
        sent_message_id = context.user_data.get('start_message_id')
        if sent_message_id:
            await context.bot.edit_message_reply_markup(
                chat_id=update.effective_chat.id,
                message_id=sent_message_id,
                reply_markup=None
            )
        context.user_data['start_message_edited_status'] = True

    if context.user_data.get('retry_message_edited_status') == False:
        retry_message_id = context.user_data.get('retry_message_id')
        if retry_message_id:
            await context.bot.edit_message_reply_markup(
                chat_id=update.effective_chat.id,
                message_id=retry_message_id,
                reply_markup=None
            )
        context.user_data['retry_message_edited_status'] = True

    if update.message and update.message.text:
        user_input = update.message.text
        loading_message = await update.message.reply_text("🔄 Fetching suggestions for your destination...")
        suggestions = google_maps.get_autocomplete_place(user_input)
        # search_one_map_location(user_input)
        if suggestions:
            filtered_suggestions = [s for s in suggestions if len(s['place_id']) <= 64]
            # Create a list of buttons with suggestions for the user to choose from
            keyboard = [[InlineKeyboardButton(suggestion['description'], callback_data=suggestion['place_id'])]
            for suggestion in filtered_suggestions]

            # Add a 'Search another destination' button at the bottom
            keyboard.append([InlineKeyboardButton("🔍 Search another destination", callback_data="search_again")])

            # Add a "End Session" button at the bottom
            keyboard.append([InlineKeyboardButton("🛑 End Session", callback_data="end")])

            reply_markup = InlineKeyboardMarkup(keyboard)

            destination_message = await context.bot.edit_message_text(
                chat_id=update.effective_chat.id,
                message_id=loading_message.message_id,
                text="🌐 *Please select your destination below:*",
                reply_markup=reply_markup,
                parse_mode="Markdown"
            )

            context.user_data['destination_message_id'] = destination_message.message_id
        else:
            await context.bot.edit_message_text(
                chat_id=update.effective_chat.id,
                message_id=loading_message.message_id,
                text='🚫 No suggestions found. Please try again.')

        return DESTINATION

async def destination_selected(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle the selected destination, search another destination, or cancel"""
    context.user_data['carpark_list_sent'] = False

    query = update.callback_query
    await query.answer()

    destination_id = query.data

    # Debug print statements to check callback data
    print(f"Callback data received: {destination_id}")

    # Check if the destination_id matches any special cases
    if destination_id == "start":
        return await start(update, context)

    if destination_id == "search_again":
        keyboard = [[InlineKeyboardButton("🛑 End Session", callback_data="end")]]

        reply_markup = InlineKeyboardMarkup(keyboard)

        retry_message = await query.edit_message_text(
            "🔄 *Let's try again.* Where would you like to go?\n\n"
            "Please type your destination.",parse_mode="Markdown",
            reply_markup=reply_markup
        )

        context.user_data['retry_message_id'] = retry_message.message_id
        context.user_data['retry_message_edited_status'] = False

        return DESTINATION

    if destination_id == "end":
        return await end(update, context)

    # Debugging to check if place details are fetched
    print(f"Destination selected. Fetching details for place ID: {destination_id}")

    try:
        # Fetch destination details using the Google Maps API
        global destination_details
        destination_details = google_maps.get_details_place(destination_id)

        if destination_details:
            lat = destination_details['geometry']['location']['lat']
            lng = destination_details['geometry']['location']['lng']
            place_name = destination_details.get('name', 'Unknown location')
            address = destination_details.get('formatted_address', 'No address available')

            context.user_data['destination_lat'] = lat
            context.user_data['destination_long'] = lng
            context.user_data['destination_address'] = place_name + " " + address

            # Display the destination details to the user
            destination_address = await query.edit_message_text(
                f"📍 *{place_name} {address}*\n\n",
                parse_mode="Markdown"
                )
            context.user_data['destination_address_id'] = destination_address.message_id

            # Generate static map URL and send the map to the user
            static_map_url = google_maps.generate_static_map_url(lat, lng)
            map_message = await context.bot.send_photo(chat_id=query.message.chat_id, photo=static_map_url)
            context.user_data['static_map_message_id'] = map_message.message_id

            # Debugging: Check if the photo is being sent
            print("Static map photo sent")

            # Create a keyboard with Yes/No options for the user to confirm
            keyboard = [
                [InlineKeyboardButton("✅ Yes", callback_data="confirm_yes"), InlineKeyboardButton("❌ No", callback_data="confirm_no")],
                [InlineKeyboardButton("🛑 End Session", callback_data="end")]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)

            # Send the confirmation message
            await query.message.reply_text("💬 *Is this the correct destination?*", reply_markup=reply_markup, parse_mode="Markdown")

            return USER_PREFERENCE
        else:
            await query.edit_message_text("❌ An error occurred. Please try again.")
            return DESTINATION
    except Exception as e:
        logger.error(f"An error occurred in get_details_place: {e}")
        await query.edit_message_text("❌ An error occurred. Please try again.")
        return DESTINATION

async def user_preference(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Obtain User's preference for carpark"""
    query = update.callback_query
    await query.answer()

    context.user_data["confirm_destination"] = query.data

    if query.data == "confirm_yes":
        print("User confirmed the location. Asking for User Preference.")

        keyboard = [
            [InlineKeyboardButton("💸Cheapest", callback_data="cheapest")],
            [InlineKeyboardButton("☂️Sheltered", callback_data="sheltered")],
            [InlineKeyboardButton("No Preference", callback_data="no_preference")],
            [InlineKeyboardButton("🛑 End Session", callback_data="end")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)

        await query.message.reply_text(
            "😄 *Would you like to indicate a preference?*\n\n By default, it is sorted by distance",
            parse_mode='Markdown',
            reply_markup=reply_markup
        )

        return CONFIRM_DESTINATION

    elif context.user_data.get("confirm_destination") == "confirm_no":
        print("User rejected the location. Asking for a new destination.")

        keyboard = [[InlineKeyboardButton("🛑 End Session", callback_data="end")]]
        reply_markup = InlineKeyboardMarkup(keyboard)

        static_map_message_id = context.user_data.get('static_map_message_id')
        if static_map_message_id:
            await context.bot.delete_message(
                chat_id=query.message.chat_id,
                message_id=static_map_message_id
            )

        await query.edit_message_text(
            "❌ *Destination rejected.* Let's search again. Where would you like to go?\n\n"
            "Please type your destination.",
            parse_mode="Markdown",
            reply_markup=reply_markup
        )

        return DESTINATION

async def confirm_destination(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Confirm the destination and ask for live location."""
    context.job_queue.stop()
    query = update.callback_query
    await query.answer()

    context.user_data["user_preference"] = query.data

    print(f"User selected: {query.data}")

    if context.user_data.get("confirm_destination") == "confirm_yes":
        print(f"User selected preference {query.data}")
        print("Asking for live location.")

        keyboard = [[InlineKeyboardButton("🛑 End Session", callback_data="end")]]
        reply_markup = InlineKeyboardMarkup(keyboard)

        confirm_destination = await query.edit_message_text(
            "✅ Destination confirmed! Please share your live location to help me find the best route.\n\n"
            "*Follow these steps:*\n"
            "📎 Paper Clip > Location > Share Live Location > Select ‘for 1 hour’",
            parse_mode="Markdown",
            reply_markup=reply_markup
        )

        context.user_data['confirm_destination_message_id'] = confirm_destination.message_id
        context.user_data['confirm_destination_edited_status'] = False

        return LIVE_LOCATION

    elif query.data == "confirm_no":
        print("User rejected the location. Asking for a new destination.")

        keyboard = [[InlineKeyboardButton("🛑 End Session", callback_data="end")]]
        reply_markup = InlineKeyboardMarkup(keyboard)

        static_map_message_id = context.user_data.get('static_map_message_id')
        if static_map_message_id:
            await context.bot.delete_message(
                chat_id=query.message.chat_id,
                message_id=static_map_message_id
            )

        await query.edit_message_text(
            "❌ *Destination rejected.* Let's search again. Where would you like to go?\n\n"
            "Please type your destination.",
            parse_mode="Markdown",
            reply_markup=reply_markup
        )

        return DESTINATION

    elif query.data == "end":
        static_map_message_id = context.user_data.get('static_map_message_id')
        destination_address_id = context.user_data.get('destination_address_id')
        if static_map_message_id:
            await context.bot.delete_message(
                chat_id=query.message.chat_id,
                message_id=static_map_message_id
            )
        if destination_address_id:
            await context.bot.delete_message(
                chat_id=query.message.chat_id,
                message_id=destination_address_id
            )

        return await end(update, context)

    return ConversationHandler.END

async def live_location(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle the live location input and find nearest carpark based on destination"""
    confirm_destination_message_id = context.user_data.get('confirm_destination_message_id')

    if context.user_data.get('confirm_destination_edited_status') == False:
        if confirm_destination_message_id:
            await context.bot.delete_message(
                chat_id=update.effective_chat.id,
                message_id=confirm_destination_message_id,
            )
            context.user_data['confirm_destination_edited_status'] = True

    query = update.callback_query
    if query and query.data == "end":
        static_map_message_id = context.user_data.get('static_map_message_id')
        if static_map_message_id:
            await context.bot.delete_message(
                chat_id=query.message.chat_id,
                message_id=static_map_message_id
            )

        return await end(update, context)

    # Handle both regular and live location updates
    if update.message and update.message.location:
        live_location = (update.message.location.latitude, update.message.location.longitude)
        context.user_data['live_location'] = live_location
        context.user_data['live_location_message_id'] = update.message.message_id
        print(Fore.BLUE + f"Received initial live location: Latitude {live_location[0]}, Longitude {live_location[1]}")
    elif update.edited_message and update.edited_message.location:
        live_location = (update.edited_message.location.latitude, update.edited_message.location.longitude)
        context.user_data['live_location'] = live_location
        print(Fore.RED + f"Received updated live location: Latitude {live_location[0]}, Longitude {live_location[1]}")
    else:
        await update.message.reply_text("⚠️ Please share your live location to proceed.")
        return LIVE_LOCATION

    if context.user_data.get('carpark_list_sent'):
        logger.info("Carpark list has already been sent. Skipping...")
        return LIVE_LOCATION

    destination_lat = context.user_data.get('destination_lat')
    destination_long = context.user_data.get('destination_long')
    if destination_lat and destination_long:
        global nearest_carparks
        nearest_carparks = geoquery_ngsi_point(
            input_type="Carpark",
            maxDistance=3000,
            lat=destination_lat,
            long=destination_long
        )

        if len(nearest_carparks) == 0:
            await context.bot.send_message(chat_id=update.effective_chat.id, text="🚫 Sorry! No nearby carparks found.")
        else:
            user_selected_preference = context.user_data.get("user_preference")
            global closest_three_carparks
            closest_three_carparks = find_closest_three_carparks(
                nearest_carparks_list=nearest_carparks,
                dest_lat=destination_lat,
                dest_long=destination_long,
                selected_preference=user_selected_preference
            )


            carparks_message = aggregate_message(closest_three_carparks, user_selected_preference)

            # today = datetime.today().weekday()
            # current_time = datetime.now().time()

            # closest_carparks_message = ""

            # for count, carpark in enumerate(closest_three_carparks, 1):
            #     carpark_name = carpark['CarparkName']['value'].title()

            #     closest_carparks_message += (
            #         f"*{count}. {carpark_name}*\n"
            #         f"🅿️ *Available Lots:* {carpark['ParkingAvailability']['value']}\n"
            #         f"📏 *Distance:* {carpark['distance']:.2f} km\n"
            #         f"☂️ *Sheltered:* {'Yes' if carpark['Sheltered']['value'] else 'No'}\n"
            #     )

            #     if 'Pricing' in carpark and 'Car' in carpark['Pricing']["value"]:
            #         if 0 <= today <= 4:  # Monday to Friday (Weekday)
            #             rate_info = find_rate_based_on_time(carpark, "Car", current_time, today)

            #             if rate_info:
            #                 minutes = int(rate_info['weekdayMin'].split(" ")[0])
            #                 h, mins = convert_to_hours(minutes)
            #                 day_type = "Weekday"
            #                 rate_display = format_time_and_rate(h, mins, rate_info['weekdayRate'])
            #                 closest_carparks_message += (
            #                 f"🏷️ *{day_type} Rate:* {rate_display}\n"
            #                 f"⏰ *Time:* {rate_info['startTime']} - {rate_info['endTime']}\n\n")
            #             else:
            #                 closest_carparks_message += "🏷️ *Price Information:* Not Available\n\n"

            #         elif today == 5:  # Saturday
            #             rate_info = find_rate_based_on_time(carpark, "Car", current_time)

            #             if rate_info:
            #                 minutes = int(rate_info['satdayMin'].split(" ")[0])
            #                 h, mins = convert_to_hours(minutes)
            #                 day_type = "Saturday"
            #                 rate_display = format_time_and_rate(h, mins, rate_info['satdayRate'])
            #                 closest_carparks_message += (
            #                 f"🏷️ *{day_type} Rate:* {rate_display}\n"
            #                 f"⏰ *Time:* {rate_info['startTime']} - {rate_info['endTime']}\n\n")
            #             else:
            #                 closest_carparks_message += "🏷️ *Price Information:* Not Available\n\n"

            #         else:  # Sunday/Public Holiday (today == 6)
            #             rate_info = find_rate_based_on_time(carpark, "Car", current_time)

            #             if rate_info:
            #                 minutes = int(rate_info['sunPHMin'].split(" ")[0])
            #                 h, mins = convert_to_hours(minutes)
            #                 day_type = "Sunday/Public Holiday"
            #                 rate_display = format_time_and_rate(h, mins, rate_info['sunPHRate'])
            #                 closest_carparks_message += (
            #                 f"🏷️ *{day_type} Rate:* {rate_display}\n"
            #                 f"⏰ *Time:* {rate_info['startTime']} - {rate_info['endTime']}\n\n")
            #             else:
            #                 closest_carparks_message += "🏷️ *Price Information:* Not Available\n\n"

            #     else:
            #         closest_carparks_message += "🏷️ *Price Information:* Not Available\n\n"

            carpark_options_message_id = await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text=carparks_message,
                )

            context.user_data['carpark_options_message_id'] = carpark_options_message_id.message_id

            context.user_data['closest_carparks'] = closest_three_carparks

            keyboard = [
                [InlineKeyboardButton(carpark['CarparkName']['value'].title(), callback_data=f"carpark_{count}")]
                for count, carpark in enumerate(closest_three_carparks)
            ]

            keyboard.append([InlineKeyboardButton("🛑 End Session", callback_data="end")])

            reply_markup = InlineKeyboardMarkup(keyboard)

            carpark_select_message = await context.bot.send_message(chat_id=update.effective_chat.id, text="Please select a carpark:", reply_markup=reply_markup)

            context.user_data['carpark_select_message_id'] = carpark_select_message.message_id

            context.user_data['carpark_list_sent'] = True

            return LIVE_LOCATION
    else:
        await context.bot.send_message(chat_id=update.effective_chat.id, text="⚠️ No destination set. Please set your destination first.")
        return LIVE_LOCATION

async def monitor_carpark_availability(update: Update, context: ContextTypes.DEFAULT_TYPE, selected_carpark):
    """Monitor the user's proximity to the selected carpark and alert if availability is low."""
    chat_id = update.message.chat_id if update.message else update.callback_query.message.chat_id

    traffic_advisories = get_traffic_advisories()
    warning_distance_km = 2

    rain_values = ["Light Rain" , "Moderate Rain" , "Heavy Rain" , "Passing Showers" , "Light Showers" , "Showers", "Heavy Showers", "Thundery Showers", "Heavy Thundery Showers", "Heavy Thundery Showers with Gusty Winds"]

    destination_lat = context.user_data.get('destination_lat')
    destination_long = context.user_data.get('destination_long')

    approaching_message_sent = False

    sent_new = False
    # Begin monitoring the user's proximity
    while True:
        # Continuously get updated live location
        live_location = context.user_data.get('live_location')
        print(context.user_data, "USER DATA")
        # Debugging: print current lat/lng for testing
        print(Fore.GREEN + f"Monitoring live location: Latitude {live_location[0]}, Longitude {live_location[1]}")

        # Check if live location is still available
        if not live_location:
            await context.bot.send_message(chat_id=chat_id, text="⚠️ Error: Couldn't retrieve your live location.")
            break

        carpark_lat = selected_carpark['location']['value']['coordinates'][1]
        carpark_long = selected_carpark['location']['value']['coordinates'][0]
        distance_to_carpark = geodesic(live_location, (carpark_lat, carpark_long)).km
        distance_to_destination = geodesic(live_location, (destination_lat, destination_long)).km

        # Debugging: print distance calculation
        print(Fore.RED + f"Distance from carpark: {distance_to_carpark:.2f} km")
        print(Fore.RED + f"Distance from destination: {distance_to_destination:.2f} km")

        # Check if the user has reached the destination
        if distance_to_carpark <= 0.1:
            await context.bot.send_message(
                chat_id=chat_id,
                text=f"🚗 You have reached your destination! 🏁 Ending the session now. Safe travels!",
                parse_mode='Markdown'
            )
            await end(update, context)
            break

        # Trigger warning if within 2km and less than 10 parking spots
        carpark_name = selected_carpark['CarparkName']['value'].title()
        available_lots = selected_carpark['ParkingAvailability']['value']

        if distance_to_carpark <= 2.0 and not approaching_message_sent:
            if available_lots < 10:
                next_best_carpark = find_next_best_carpark(context.user_data['closest_carparks'], selected_carpark)

                if next_best_carpark:
                    next_carpark_name = next_best_carpark['CarparkName']['value'].title()
                    next_carpark_lat = next_best_carpark['location']['value']['coordinates'][1]
                    next_carpark_long = next_best_carpark['location']['value']['coordinates'][0]

                    google_maps_link = (
                        f"https://www.google.com/maps/dir/?api=1&origin={live_location[0]},{live_location[1]}"
                        f"&waypoints={next_carpark_lat},{next_carpark_long}"
                        f"&destination={context.user_data.get('destination_lat')},{context.user_data.get('destination_long')}&travelmode=driving"
                    )

                    await context.bot.send_message(
                        chat_id=chat_id,
                        text=(
                            f"🚗 You are approaching your destination, but ⚠️ warning! The carpark '{carpark_name}' "
                            f"has less than 10 available lots left. We suggest using the next closest carpark '{next_carpark_name}' instead.\n\n"
                            f"[Click here to view the route]({google_maps_link})"
                        ),
                        parse_mode='Markdown',
                        disable_web_page_preview=True
                    )
                else:
                    await context.bot.send_message(
                    chat_id=chat_id,
                    text=f"🚗 You are approaching your destination, but ⚠️ warning! The carpark '{carpark_name}' has less than 10 available lots left. Drive carefully!",
                    parse_mode='Markdown'
                )
            else:
                await context.bot.send_message(
                    chat_id=chat_id,
                    text=f"🚗 You are approaching your destination, and the carpark '{carpark_name}' currently has {available_lots} available lots. Drive carefully!",
                    parse_mode='Markdown'
                )

            approaching_message_sent = True

        # for advisory in traffic_advisories:
        #     advisory_coordinates = advisory['Location']['value']['coordinates']
        #     advisory_lat = advisory_coordinates[1]
        #     advisory_long = advisory_coordinates[0]
        #     distance_to_advisory = geodesic(live_location, (advisory_lat, advisory_long)).km

        #     print(Fore.BLUE + f"Distance to advisory {advisory['id']}: {distance_to_advisory:.2f} km")

        #     if distance_to_advisory <= warning_distance_km:
        #         advisory_message = advisory['Message']['value']
        #         await context.bot.send_message(
        #             chat_id=chat_id,
        #             text=f"🚧 *Traffic Advisory:* {advisory_message}",
        #             parse_mode='Markdown'
        #         )
        # Sleep for 5 seconds before checking again
        await asyncio.sleep(5)
        if sent_new == True:
            break

async def monitor_weather(update: Update, context: ContextTypes.DEFAULT_TYPE):
    rain_values = ["Light Rain" , "Moderate Rain" , "Heavy Rain" , "Passing Showers" , "Light Showers" , "Showers", "Heavy Showers", "Thundery Showers", "Heavy Thundery Showers", "Heavy Thundery Showers with Gusty Winds"]

    weather = [
        {
            "id": "urn:ngsi-ld:WeatherForecast:Bedok-WeatherForecast-2024-10-08T12:23:56_2024-10-08T14:23:56",
            "type": "WeatherForecast",
            "Area": {
                "type": "Property",
                "value": "Bedok"
            },
            "forecast": {
                "type": "Property",
                "value": "Heavy Rain"
            },
            "location": {
                "type": "GeoProperty",
                "value": {
                    "type": "Point",
                    "coordinates": [
                        103.924,
                        1.321
                    ]
                }
            }
        }
        ]
    carpark_location = current_carpark['location']['value']['coordinates']

    query = update.callback_query
    await query.answer()

    while True:
        live_location = context.user_data.get('live_location')
        for area in weather:
            carpark_coordinates = (carpark_location[1], carpark_location[0])
            forecast_coordinates= (area["location"]["value"]["coordinates"][1], area["location"]["value"]["coordinates"][0])

            distance = geodesic(carpark_coordinates, forecast_coordinates).km

            check_distance_list = [0.5, 1.0, 1.5, 2.0]
            new_carpark = None
            for check_distance in check_distance_list:
                if distance < check_distance and area["forecast"]["value"] in rain_values:
                    rain_value = area["forecast"]["value"]
                    print("rain_value:", rain_value)
            if current_carpark["Sheltered"]["value"] == False:
                new_carpark = find_closest_carpark(closest_three_carparks, destination_details['geometry']['location']['lat'], destination_details['geometry']['location']['lng'])

                print(new_carpark)

                lat = new_carpark["location"]["value"]["coordinates"][1]
                long = new_carpark["location"]["value"]["coordinates"][0]
                google_maps_link = (
                    f"https://www.google.com/maps/dir/?api=1&origin={live_location[0]},{live_location[1]}"
                    f"&waypoints={lat},{long}"
                    f"&destination={context.user_data.get('destination_lat')},{context.user_data.get('destination_long')}&travelmode=driving"
                )

                await asyncio.sleep(5)

                await query.message.reply_text(
                    f"🌧️ *Looks like it is raining and your destination is not sheltered:*\n\n"
                    f"🛣️ *Here is a new route to a sheltered carpark:*\n\n"
                    f"📍 Start: {user_address}\n"
                    f"🅿️ Stop: {new_carpark['CarparkName']['value'].title()} (Carpark)\n"
                    f"🏁 End: {destination_address}\n\n"
                    f"[Click here to view the route]({google_maps_link})",
                    parse_mode='Markdown',
                    disable_web_page_preview=True
                )
                sent_new = True
                break
            else:
                await context.bot.send_message(
                    chat_id=update.effective_chat.id,
                    text=f"🌦️ *Weather Update:* There is an ongoing {rain_value} happening around your destination. Drive safely and remember to grab an umbrella!",
                    parse_mode='Markdown')
                break

        # Sleep for 5 seconds before checking again
        await asyncio.sleep(5)
        if sent_new == True:
            break

async def monitor_traffic_advisories(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Monitor traffic advisories along the route."""
    chat_id = update.message.chat_id if update.message else update.callback_query.message.chat_id
    # traffic_advisories = get_traffic_advisories()

    mock_traffic_advisories = {
        "id": "urn:ngsi-ld:TrafficAdvisories:EVMS_RD10",
        "type": "TrafficAdvisories",
        "Message": {
            "type": "Property",
            "value": "DRIVE SAFELY,SPEED CAMERAS,IN TUNNEL"
        },
        "Location": {
            "type": "GeoProperty",
            "value": {
                "type": "Point",
                "coordinates": [
                    103.5955,
                    1.29548
                ]
            }
        }
    }

    while True:
        live_location = context.user_data.get('live_location')

        # if not live_location:
        #     await context.bot.send_message(chat_id=chat_id, text="⚠️ Error: Couldn't retrieve your live location.")
        #     break

        # Example logic for traffic advisory monitoring
        # for advisory in traffic_advisories:
        #     advisory_coordinates = advisory['Location']['value']['coordinates']
        #     advisory_lat = advisory_coordinates[1]
        #     advisory_long = advisory_coordinates[0]
        #     distance_to_advisory = geodesic(live_location, (advisory_lat, advisory_long)).km

        #     print(Fore.RED + str(distance_to_advisory))

        #     if distance_to_advisory <= 1.0:
        #         advisory_message = advisory['Message']['value']
        #         await context.bot.send_message(
        #             chat_id=chat_id,
        #             text=f"🚧 Traffic advisory: {advisory_message}",
        #             parse_mode='Markdown'
        #         )

        advisory_coordinates = mock_traffic_advisories['Location']['value']['coordinates']
        advisory_lat = advisory_coordinates[1]
        advisory_long = advisory_coordinates[0]
        distance_to_advisory = geodesic(live_location, (advisory_lat, advisory_long)).km

        print(Fore.RED + f"Distance to advisory: {distance_to_advisory:.2f} km")

        if distance_to_advisory <= 1.0:
            advisory_message = mock_traffic_advisories['Message']['value']
            await context.bot.send_message(
                chat_id=chat_id,
                text=f"🚧 Traffic advisory: {advisory_message}",
                parse_mode='Markdown'
            )
            break

        await asyncio.sleep(5)

async def monitor_all(update: Update, context: ContextTypes.DEFAULT_TYPE, selected_carpark):
    """Run all monitoring tasks concurrently."""
    await asyncio.gather(
        monitor_carpark_availability(update, context, selected_carpark),
        monitor_traffic_advisories(update, context),
        monitor_weather(update, context)
    )

async def carpark_selected(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle the selected carpark and return a Google Maps route."""
    query = update.callback_query
    await query.answer()

    if query.data == "start":
        return await start(update, context)

    if query.data == "end":
        static_map_message_id = context.user_data.get('static_map_message_id')
        if static_map_message_id:
            try:
                await context.bot.delete_message(
                    chat_id=query.message.chat_id,
                    message_id=static_map_message_id
                )
            except BadRequest as e:
                logger.error(f"Failed to delete static map message: {e}")

        carpark_options_message_id = context.user_data.get('carpark_options_message_id')
        if carpark_options_message_id:
            try:
                await context.bot.delete_message(
                    chat_id=update.effective_chat.id,
                    message_id=carpark_options_message_id
                )
            except BadRequest as e:
                logger.error(f"Failed to delete carpark options message: {e}")

        destination_address_id = context.user_data.get('destination_address_id')
        if destination_address_id:
            try:
                await context.bot.delete_message(
                    chat_id=update.effective_chat.id,
                    message_id=destination_address_id
                )
            except BadRequest as e:
                logger.error(f"Failed to delete destination address message: {e}")

        return await end(update, context)

    carpark_options_message_id = context.user_data.get('carpark_options_message_id')
    if carpark_options_message_id:
        try:
            await context.bot.delete_message(
                chat_id=update.effective_chat.id,
                message_id=carpark_options_message_id
            )
        except BadRequest as e:
            logger.error(f"Failed to delete carpark options message: {e}")

    carpark_select_message_id = context.user_data.get('carpark_select_message_id')
    if carpark_select_message_id:
        await context.bot.delete_message(
            chat_id=update.effective_chat.id,
            message_id=carpark_select_message_id
        )

    selected_carpark_index = int(query.data.split("_")[1])
    closest_three_carparks = context.user_data['closest_carparks']
    selected_carpark = closest_three_carparks[selected_carpark_index]

    live_location = context.user_data.get('live_location')
    if not live_location:
        await query.message.reply_text("⚠️ Error: Couldn't retrieve your live location.")
        return ConversationHandler.END

    global user_address
    global destination_address

    user_address = google_maps.get_address_from_coordinates(live_location[0], live_location[1])
    destination_address = context.user_data.get('destination_address')

    carpark_lat = selected_carpark['location']['value']['coordinates'][1]
    carpark_long = selected_carpark['location']['value']['coordinates'][0]
    destination_lat = context.user_data.get('destination_lat')
    destination_long = context.user_data.get('destination_long')

    google_maps_link = (
        f"https://www.google.com/maps/dir/?api=1&origin={live_location[0]},{live_location[1]}"
        f"&waypoints={carpark_lat},{carpark_long}"
        f"&destination={destination_lat},{destination_long}&travelmode=driving"
    )

    keyboard = [[InlineKeyboardButton("🛑 End Session", callback_data="end")]]
    reply_markup = InlineKeyboardMarkup(keyboard)

    google_route_id = await query.message.reply_text(
        f"🛣️ *Here is your route:*\n\n"
        f"📍 Start: {user_address}\n"
        f"🅿️ Stop: {selected_carpark['CarparkName']['value'].title()} (Carpark)\n"
        f"🏁 End: {destination_address}\n\n"
        f"[Click here to view the route]({google_maps_link})",
        parse_mode='Markdown',
        reply_markup=reply_markup,
        disable_web_page_preview=True
    )

    context.user_data['google_route_id'] = google_route_id.message_id

    # asyncio.create_task(monitor_carpark_availability(update, context, selected_carpark))
    asyncio.create_task(monitor_all(update, context, selected_carpark))

    global current_carpark
    current_carpark = selected_carpark

    return LIVE_LOCATION

async def end(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle ending the session and provide a restart button."""
    # destination_address_id = context.user_data.get('destination_address_id')
    # if destination_address_id:
    #     try:
    #         await context.bot.delete_message(
    #             chat_id=update.effective_chat.id,
    #             message_id=destination_address_id
    #         )
    #     except BadRequest as e:
    #         logger.error(f"Failed to delete destination address message: {e}")

    # static_map_message_id = context.user_data.get('static_map_message_id')
    # if static_map_message_id:
    #     try:
    #         await context.bot.delete_message(
    #             chat_id=update.effective_chat.id,
    #             message_id=static_map_message_id
    #         )
    #     except BadRequest as e:
    #         logger.error(f"Failed to delete static map message: {e}")

    live_location_message_id = context.user_data.get('live_location_message_id')
    if live_location_message_id:
        try:
            await context.bot.delete_message(
                chat_id=update.effective_chat.id,
                message_id=live_location_message_id
            )
        except BadRequest as e:
            logger.error(f"Failed to delete live location message: {e}")

    google_route_id = context.user_data.get('google_route_id')
    if google_route_id:
        try:
            await context.bot.edit_message_reply_markup(
                chat_id=update.effective_chat.id,
                message_id=google_route_id,
                reply_markup=None
            )
        except BadRequest as e:
            logger.error(f"Failed to delete Google route message: {e}")

    context.user_data.clear()

    if update.message:
        await update.message.reply_text('👋 *Goodbye!* I look forward to assisting you again.', parse_mode="Markdown", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔁 Start Session", callback_data="start")]]))

    elif update.callback_query:
        query = update.callback_query
        try:
            await query.answer()

            if query.message:
                await query.edit_message_text("👋 *Goodbye!* I look forward to assisting you again.", parse_mode="Markdown", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔁 Start Session", callback_data="start")]])
                )
            else:
                await context.bot.send_message(
                    chat_id=query.message.chat_id,
                    text="👋 *Goodbye!* I look forward to assisting you again.",
                    parse_mode="Markdown",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔁 Start Session", callback_data="start")]])
                )
        except BadRequest as e:
            logger.error(f"Failed to edit message or answer callback query: {e}")
            await context.bot.send_message(
                chat_id=query.message.chat_id,
                text="👋 *Goodbye!* I look forward to assisting you again.",
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔁 Start Session", callback_data="start")]])
            )

    return RESTART_SESSION

async def restart_session(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Restart the bot session when the user clicks 'Start Session'."""
    query = update.callback_query
    await query.answer()

    return await start(update, context)

def find_closest_three_carparks(nearest_carparks_list, dest_lat, dest_long, selected_preference):
    closest_three_carparks = []
    distance_dict = {}
    final_three_carparks = []

    for carpark in nearest_carparks_list:
        carpark_dict = carpark.to_dict()
        lat = carpark_dict["location"]["value"]["coordinates"][1]
        long = carpark_dict["location"]["value"]["coordinates"][0]
        distance = geodesic((dest_lat, dest_long), (lat, long)).km
        distance_dict[carpark_dict["CarparkName"]["value"]] = distance
        carpark_dict["distance"] = distance
        if "Car" in carpark_dict["Pricing"]["value"] and carpark_dict["ParkingAvailability"]["value"] > 0:
            if selected_preference == "sheltered":
                if carpark["Sheltered"]["value"] == True:
                    if len(closest_three_carparks) < 3:
                        closest_three_carparks.append(carpark_dict)
                    else:
                        farthest_carpark = max(closest_three_carparks, key=lambda x: x["distance"])
                        print("farthest_carpark:", farthest_carpark, "farthest_carpark distance:", farthest_carpark["distance"])
                        if farthest_carpark["distance"] > carpark_dict["distance"]:
                            closest_three_carparks.remove(farthest_carpark)
                            closest_three_carparks.append(carpark_dict)
            else:
                if len(closest_three_carparks) < 3:
                    closest_three_carparks.append(carpark_dict)
                else:

                    farthest_carpark = max(closest_three_carparks, key=lambda x: x["distance"])
                    # print("carpark_dict:", carpark_dict)
                    # print("farthest_carpark:", farthest_carpark, "farthest_carpark distance:", farthest_carpark["distance"])
                    if farthest_carpark["distance"] > carpark_dict["distance"]:
                        closest_three_carparks.remove(farthest_carpark)
                        closest_three_carparks.append(carpark_dict)

    # Sort the closest_three_carparks based on distance
    closest_three_carparks.sort(key=lambda x: distance_dict[x["CarparkName"]["value"]])

    # Add the sorted carparks to the final_three_carparks list
    final_three_carparks.extend(closest_three_carparks)
    # for i in final_three_carparks:
    #     print("carparks:", i["CarparkName"]["value"], i["distance"])

    return final_three_carparks
    # return closest_three_carparks

def find_closest_carpark(carparks_list, dest_lat, dest_long):
    in_list = False
    for carpark in carparks_list:
        if carpark["Sheltered"]["value"] == True:
            in_list = True
            return carpark

    if in_list == False:
        distance_dict = {}
        selected_carpark = []

        lat = carpark["location"]["value"]["coordinates"][1]
        long = carpark["location"]["value"]["coordinates"][0]

        for carpark in nearest_carparks:
            distance = geodesic((dest_lat, dest_long), (lat, long)).km
            distance_dict[carpark["CarparkName"]["value"]] = distance

        distance_dict.sort(key=lambda x: distance_dict[x["CarparkName"]["value"]])

        # Add the sorted carparks to the final_three_carparks list
        selected_carpark.extend(closest_three_carparks)
        print("selected_carpark:", selected_carpark[0])
        return selected_carpark[0]


def get_traffic_advisories():
    return retrieve_ngsi_type("TrafficAdvisories")

def get_weather():
    return retrieve_ngsi_type("WeatherForecast")

def format_time_and_rate(h, mins, rate):
    if rate == "$0.00":
        return "Free"

    time_string = ""
    if h > 0:
        time_string += f"{h} h "
    if mins > 0:
        time_string += f"{mins} mins"

    return f"{rate} per {time_string.strip()}" if time_string.strip() else rate

def convert_to_hours(minutes):
    hours = minutes // 60
    minutes = minutes % 60
    return hours, minutes

def is_time_in_range(start_time, end_time, current_time):
    start = datetime.strptime(start_time, "%H%M").time()
    end = datetime.strptime(end_time, "%H%M").time()
    return start <= current_time <= end

def find_rate_based_on_time(carpark, vehicle_type, current_time, today):
    time_slots = carpark['Pricing']['value'][vehicle_type]['TimeSlots']

    if 0 <= today <= 4:
        rate_type = "WeekdayRate"
    elif today == 5:
        rate_type = "SaturdayRate"
    else:
        rate_type = "SundayPHRate"

    for time_slot in time_slots:
        rate_info = time_slot[rate_type]
        start_time = rate_info["startTime"]
        end_time = rate_info["endTime"]

        if start_time and end_time and is_time_in_range(start_time, end_time, current_time):
            return rate_info

    return None

def aggregate_message(closest_three_carparks, selected_preference):
    carparks_message = "🚗 *The 3 possible carparks near your destination are:*\n\n"

    today = datetime.today().weekday()
    current_time = datetime.now().time()

    price_dict = {}

    for count, carpark in enumerate(closest_three_carparks, 1):
        carpark_name = carpark['CarparkName']['value'].title()
        if 'Pricing' in carpark and 'Car' in carpark['Pricing']["value"]:

            carparks_message += (
                    f"*{count}. {carpark_name}*\n"
                    f"🅿️ *Available Lots:* {carpark['ParkingAvailability']['value']}\n"
                    f"📏 *Distance:* {carpark['distance']:.2f} km\n"
                    f"☂️ *Sheltered:* {'Yes' if carpark['Sheltered']['value'] else 'No'}\n"
                )

            if 0 <= today <= 4:  # Monday to Friday (Weekday)
                rate_info = find_rate_based_on_time(carpark, "Car", current_time, today)

                if rate_info:
                    price = rate_info['weekdayRate']
                    price_dict[carpark_name] = price

                    minutes = int(rate_info['weekdayMin'].split(" ")[0])
                    h, mins = convert_to_hours(minutes)
                    day_type = "Weekday"
                    rate_display = format_time_and_rate(h, mins, rate_info['weekdayRate'])
                    carparks_message += (
                    f"🏷️ *{day_type} Rate:* {rate_display}\n"
                    f"⏰ *Time:* {rate_info['startTime']} - {rate_info['endTime']}\n\n")
                else:
                    carparks_message += "🏷️ *Price Information:* Not Available\n\n"

            elif today == 5:  # Saturday
                rate_info = find_rate_based_on_time(carpark, "Car", current_time)

                if rate_info:
                    price = rate_info['satdayRate']
                    price_dict[carpark_name] = price
                    minutes = int(rate_info['satdayMin'].split(" ")[0])
                    h, mins = convert_to_hours(minutes)
                    day_type = "Saturday"
                    rate_display = format_time_and_rate(h, mins, rate_info['satdayRate'])
                    carparks_message += (
                    f"🏷️ *{day_type} Rate:* {rate_display}\n"
                    f"⏰ *Time:* {rate_info['startTime']} - {rate_info['endTime']}\n\n")
                else:
                    carparks_message += "🏷️ *Price Information:* Not Available\n\n"

            else:  # Sunday/Public Holiday (today == 6)
                rate_info = find_rate_based_on_time(carpark, "Car", current_time)

                if rate_info:
                    price = rate_info['sunPHRate']
                    price_dict[carpark_name] = price
                    minutes = int(rate_info['sunPHMin'].split(" ")[0])
                    h, mins = convert_to_hours(minutes)
                    day_type = "Sunday/Public Holiday"
                    rate_display = format_time_and_rate(h, mins, rate_info['sunPHRate'])
                    carparks_message += (
                    f"🏷️ *{day_type} Rate:* {rate_display}\n"
                    f"⏰ *Time:* {rate_info['startTime']} - {rate_info['endTime']}\n\n")
                else:
                    carparks_message += "🏷️ *Price Information:* Not Available\n\n"

        else:
            carparks_message += "🏷️ *Price Information:* Not Available\n\n"

    # Process Preference message
    if selected_preference == "cheapest":
        # find out if all prices are the same
        price_list = list(price_dict.values())
        print(price_list)

        if len(set(price_list))==1:
            cheapest_carpark_message = f"💸 *All carparks have the same price at {price_list[0]} per 30 mins* \n\n"
            final_message = cheapest_carpark_message + carparks_message
            return final_message
        else:
            lowest_value_key = min(price_dict, key=price_dict.get)
            lowest_value = price_dict[lowest_value_key]
            print("Lowest Value:", lowest_value)
            print("Lowest Value Key:", lowest_value_key)
        cheapest_carpark_message = f"💸 *The cheapest carpark is:* {lowest_value_key} at {lowest_value} per 30 mins* \n\n"
        final_message = cheapest_carpark_message + carparks_message
        return final_message

    return carparks_message

def find_next_best_carpark(carparks, current_carpark):
    """Find the next best carpark with more than 10 available lots."""
    best_carpark = None
    min_distance = float('inf')

    for carpark in carparks:
        if carpark == current_carpark:
            continue

        available_lots = carpark['ParkingAvailability']['value']
        if available_lots > 10:
            distance = carpark['distance']  # Assuming you already calculated distances

            if distance < min_distance:
                min_distance = distance
                best_carpark = carpark

    return best_carpark

def main() -> None:
    """Run the bot."""
    application = ApplicationBuilder().token(constants.TELEGRAM_BOT_KEY).build()

    conv_handler = ConversationHandler(
        entry_points=[CommandHandler('start', start)],
        states={
            DESTINATION: [
                MessageHandler(filters.LOCATION | filters.TEXT, get_destination),
                CallbackQueryHandler(destination_selected),
            ],
            USER_PREFERENCE: [
                CallbackQueryHandler(user_preference),
            ],
            CONFIRM_DESTINATION: [
                CallbackQueryHandler(confirm_destination),
            ],
            LIVE_LOCATION: [
                MessageHandler(filters.LOCATION | filters.TEXT, live_location),
                CallbackQueryHandler(carpark_selected),
            ],
            RESTART_SESSION: [
                CallbackQueryHandler(restart_session, pattern='^start$'),
            ]
        },
        fallbacks=[CommandHandler('end', end),
        CallbackQueryHandler(end, pattern="^end$"),
        CallbackQueryHandler(restart_session, pattern="^start$")
        ],
    )

    application.add_handler(conv_handler)
    application.run_polling()

if __name__ == '__main__':
    main()
