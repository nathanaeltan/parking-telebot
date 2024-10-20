import sys
print(sys.version)

import constants as constants
from utils.context_broker import geoquery_ngsi_point

import logging
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes, ConversationHandler
from telegram.error import BadRequest

# import functions
from utils.monitor import monitor_all
from utils.helper_functions import find_closest_three_carparks,aggregate_message
from utils.context_broker import geoquery_ngsi_point
from utils.google_maps import get_autocomplete_place, get_details_place, generate_static_map_url, get_address_from_coordinates

import asyncio

import colorama
from colorama import Fore, Back, Style
colorama.init(autoreset=True)


# State definitions
DESTINATION, CONFIRM_DESTINATION, LIVE_LOCATION, RESTART, USER_PREFERENCE = range(5)

# Store user data
user_data = {}

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Send a welcome message and ask for user's destination."""
    keyboard = [[InlineKeyboardButton("🛑 End Session", callback_data="end")]]
    reply_markup = InlineKeyboardMarkup(keyboard)

    
    if update.message: 
        message = await update.message.reply_text(
         "👋 *Welcome!* Where would you like to go today?\n\n"
         "Please type your destination.",
         parse_mode='Markdown',
         reply_markup=reply_markup
    )

        context.user_data['message_id'] = message.message_id

    elif update.callback_query:
        await update.callback_query.edit_message_text(
           "👋 *Welcome!* Where would you like to go today?\n\n""Please type your destination.",
           parse_mode='Markdown',
           reply_markup=reply_markup
    ) 

    return DESTINATION

async def get_destination(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle destination input and return a list of suggestions"""
    if update.message and update.message.text:
        user_input = update.message.text
        loading_message = await update.message.reply_text("🔄 Fetching suggestions for your destination...")
        suggestions = get_autocomplete_place(user_input)

        keyboard = [[InlineKeyboardButton(suggestion['description'], callback_data=suggestion['place_id'][:64])]
        for suggestion in suggestions]

        keyboard.append([InlineKeyboardButton("🔍 Search another destination", callback_data="search_again")])

        keyboard.append([InlineKeyboardButton("🛑 End Session", callback_data="end")])

        reply_markup = InlineKeyboardMarkup(keyboard)

        await context.bot.edit_message_text(
            chat_id=update.effective_chat.id,
            message_id=loading_message.message_id,
            text="🌐 *Please select your destination below:*", 
            reply_markup=reply_markup,
            parse_mode="Markdown"
        )

        return DESTINATION

async def destination_selected(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle the selected destination, search another destination, or cancel"""
    # Logging setup
    logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
    logger = logging.getLogger(__name__)
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
        destination_details = get_details_place(destination_id)

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
            static_map_url = generate_static_map_url(lat, lng)
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
            confirm_destination_message = await query.message.reply_text(
                "💬 *Is this the correct destination?*",
                reply_markup=reply_markup,
                parse_mode="Markdown"
                )
            
            context.user_data['confirm_destination_message'] = confirm_destination_message.message_id

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

        confirm_destination_message_id = context.user_data.get('confirm_destination_message')
        if confirm_destination_message_id:
            await context.bot.delete_message(
                chat_id=query.message.chat_id,
                message_id=confirm_destination_message_id
            )

        keyboard = [
            [InlineKeyboardButton("💸 Cheapest", callback_data="cheapest")],
            [InlineKeyboardButton("☂️ Sheltered", callback_data="sheltered")],
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
    user_preference = None

    context.user_data["user_preference"] = query.data
    if context.user_data.get("user_preference") == "cheapest": 
        user_preference = "💸 Cheapest"
    elif context.user_data.get("user_preference") == "sheltered":
        user_preference = "☂️ Sheltered"
    elif context.user_data.get("user_preference") == "no_preference":
        user_preference = "No Preference"

    print(f"User selected: {query.data}")

    if context.user_data.get("confirm_destination") == "confirm_yes":
        print(f"User selected preference: {query.data}")
        print("Asking for live location.")
        
        keyboard = [[InlineKeyboardButton("🛑 End Session", callback_data="end")]]
        reply_markup = InlineKeyboardMarkup(keyboard)

        confirm_preference = await query.edit_message_text(
            f"You have selected *{user_preference}*",
            parse_mode="Markdown",
            reply_markup=None
        )

        context.user_data['confirm_preference_message_id'] = confirm_preference.message_id

        confirm_destination = await context.bot.send_message(
            chat_id=query.message.chat_id,
            text="✅ Destination confirmed! Please share your live location to help me find the best route.\n\n"
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

    # Logging setup
    logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
    logger = logging.getLogger(__name__)

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

            # carparks_message = aggregate_message(closest_three_carparks, user_selected_preference)
            # escaped_message = escape_special_chars(carparks_message)

            carpark_options_message_id = await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text=carparks_message,
                parse_mode='Markdown')

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

async def carpark_selected(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle the selected carpark and return a Google Maps route."""
    query = update.callback_query
    await query.answer()

    # Logging setup
    logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
    logger = logging.getLogger(__name__)

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

    context.user_data['selected_carpark_lat'] = selected_carpark['location']['value']['coordinates'][1]
    context.user_data['selected_carpark_long'] = selected_carpark['location']['value']['coordinates'][0]
    context.user_data['selected_carpark'] = selected_carpark
    context.user_data['selected_carpark_name'] = selected_carpark['CarparkName']['value'].title()

    selected_carpark_name = selected_carpark['CarparkName']['value'].title()
    await query.message.reply_text(
        f"🅿️ You have selected *{selected_carpark_name}* as your carpark.",
        parse_mode="Markdown"
    )

    live_location = context.user_data.get('live_location')
    if not live_location:
        await query.message.reply_text("⚠️ Error: Couldn't retrieve your live location.")
        return ConversationHandler.END
    
    global user_address
    global destination_address

    user_address = get_address_from_coordinates(live_location[0], live_location[1])
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
    
    global current_carpark
    current_carpark = selected_carpark

    # asyncio.create_task(monitor_carpark_availability(update, context, selected_carpark))
    asyncio.create_task(monitor_all(update, context, current_carpark))
    
    return LIVE_LOCATION

async def restart_session(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Restart the bot session when the user clicks 'Start Session'."""
    query = update.callback_query
    await query.answer()

    return await start(update, context)

async def end(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """End the session and provide a restart button."""
    message_id = context.user_data.get('message_id')

    try:
        await context.bot.edit_message_text(
            chat_id=update.effective_chat.id,
            message_id=message_id,
            text="👋 *Goodbye!* I look forward to assisting you again.",
            parse_mode='Markdown',
            reply_markup=None)
    except BadRequest as e:
        await update.callback_query.message.reply_text(
            # TODO: Add error message instead 
            "👋 *Goodbye!* I look forward to assisting you again.", parse_mode='Markdown',
            reply_markup=None
    )
    
    return ConversationHandler.END