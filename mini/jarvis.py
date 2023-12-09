import pyttsx3
from faker import Faker
engine=pyttsx3.init()
f=Faker()
voices=engine.getProperty('voices')
engine.setProperty('voice', voices[1].id) 
for voice in voices:
    print(voice)
for i in range(100):
    s=f.name()                                                                                                                                                                                          
    print(f"saying=>{s}")
    engine.say(s)
    engine.runAndWait()

