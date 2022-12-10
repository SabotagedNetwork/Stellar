import ijson.backends.yajl2_c as ijson
import asyncio
import sys
import aiomysql
from config import *


class Export:
    msgblock = []

    def __init__(self, filename):
        self.f = open(filename, "r")
        self.userlist = []
        asyncio.run(self.boot())

    async def boot(self):
        # Variables are taken from config.py
        self.pool = await aiomysql.create_pool(
            host=HOST,
            port=PORT,
            user=USER,
            password=PASSWORD,
            db=DB,
            loop=asyncio.get_event_loop(),
        )
        self.conn = await self.pool.acquire()
        self.cur = await self.conn.cursor()
        await self.parseJson()
        await self.parseUsers()

    """ Send message to database"""

    async def parseMessages(self, message):
        await self.cur.execute(
            f"INSERT INTO `messages` (UserID, Content, ReferenceID, MessageID, Attachments, timestamp) VALUES ('{message[0]}', '{message[1]}', '{message[2]}', '{message[3]}', '{message[4]}', '{message[5]}')"
        )
        await self.conn.commit()

    """ Send users to database """

    async def parseUsers(self):
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                for user in self.userlist:
                    try:
                        await cur.execute(
                            f"INSERT INTO `users` (UserID, Name, PFPurl) VALUES ('{user[0]}', '{user[1]}', '{user[2]}');"
                        )
                    # Triggers on any error from aiomysql
                    except:
                        print("caught lacking")
            await conn.commit()
        self.pool.close()
        await self.pool.wait_closed()

    """ Add a user to userlist """

    async def formUser(self, formatted):
        if formatted not in self.userlist:
            print(f"Adding {formatted}")
            self.userlist.append(formatted)

    """ Form a message block """

    async def formMessage(self, MessageBlock):
        msgID = None
        editedBool = None
        messageContent = None
        authorId = None
        authorName = None
        authorDiscrim = None
        authorAvatarUrl = None
        attachmentsUrl = None
        timeStamp = None
        referenceID = None

        for obj in MessageBlock:
            objtype = obj[0]
            match objtype:
                case "messages.item.id":
                    msgID = obj[2]
                case "messages.item.timestamp":
                    timeStamp = obj[2]
                case "messages.item.timestampEdited":
                    if obj[2] != None:
                        editedBool = "(edited)"
                case "messages.item.content":
                    messageContent = obj[2]
                    messageContent = messageContent.replace("'", r"\'").rstrip("\\")
                case "messages.item.author.id":
                    authorId = obj[2]
                case "messages.item.author.name":
                    authorName = obj[2]
                    authorName = authorName.replace("'", r"\'")
                case "messages.item.author.discriminator":
                    authorDiscrim = obj[2]
                case "messages.item.author.avatarUrl":
                    authorAvatarUrl = obj[2]
                case "messages.attachments.item.url":
                    attachmentsUrl = obj[2]
                case "messages.item.reference.messageId":
                    referenceID = obj[2]
        if None not in (authorId, authorName, authorDiscrim, authorAvatarUrl):
            await self.formUser(
                [authorId, f"{authorName}#{authorDiscrim}", authorAvatarUrl]
            )
        if None not in (authorId, messageContent, msgID, timeStamp):
            await self.parseMessages(
                [
                    authorId,
                    messageContent,
                    referenceID,
                    msgID,
                    attachmentsUrl,
                    timeStamp,
                ]
            )
        # print(f"[{timeStamp}] {authorName}#{authorDiscrim} ({authorId}): {messageContent} {editedBool} || {referenceID}")

    """ Parse the json """

    async def parseJson(self):
        for pref, type, value in ijson.parse(self.f):
            # Check if its the start of a message block
            if [pref, type, value] == ["messages.item", "start_map", None]:
                self.msgblock = []

            self.msgblock.append([pref, type, value])

            # Check if its the end of a message block
            if [pref, type, value] == ["messages.item", "end_map", None]:
                await self.formMessage(self.msgblock)


filename = sys.argv[1]
Exporter = Export(filename)
