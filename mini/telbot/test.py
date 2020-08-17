# print("1) started")
# from telegram.ext import Updater, CommandHandler
# print("2) import done")

# def hello(update, context):
#     update.message.reply_text(
#         'Hello {}'.format(update.message.from_user.first_name))

# print("3) defined")
# updater = Updater('1173949261:AAEnxCBjI9oLLqdGUlumnN7d9cU1M5ETe9o', use_context=True)
# print("4) tokenize")
# updater.dispatcher.add_handler(CommandHandler('hello', hello))
# print("5) add handler complete")
# updater.start_polling()
# print("6) started to poll")
# updater.idle()
# print("7) idling")

import requests

def telegram_bot_sendtext(bot_message):

   bot_token = '1173949261:AAEnxCBjI9oLLqdGUlumnN7d9cU1M5ETe9o'
   bot_chatID = '1190750574'
   send_text = 'https://api.telegram.org/bot' + bot_token + '/sendMessage?chat_id=' + bot_chatID + '&parse_mode=Markdown&text=' + bot_message

   response = requests.get(send_text)

   return response.json()


test = telegram_bot_sendtext("Testing Telegram bot")
print(test)