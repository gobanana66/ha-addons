const express = require('express');
const fs = require('fs');
const yaml = require('js-yaml');
const app = express();
const port = 3000;

const configRaw = fs.readFileSync('/data/options.json', 'utf8');
const config = JSON.parse(configRaw);

const prefix = config.prefix;
const clientId = config.clientId;
const guildId = config.guildId;
const token = config.token;
const { exec } = require("child_process");
const knope = require('knope');

console.log('Prefix:', prefix);
console.log('Client ID:', clientId);
console.log('Guild ID:', guildId);
console.log('Token:', token);

if (!process.env.TOKEN) {
  console.error('ERROR: TOKEN is required!');
  process.exit(1);
}


// Require the necessary discord.js classes
const { Client, Intents } = require('discord.js');

// Create a new client instance
const client = new Client({
  intents: [Intents.FLAGS.GUILDS, Intents.FLAGS.GUILD_MESSAGES, Intents.FLAGS.GUILD_MESSAGE_REACTIONS],
});


// When the client is ready, run this code (only once)
client.once('ready', () => {
  console.log('Ready!');
  client.user.setActivity({
       type: "PLAYING",
       name: "Silly Shark Things",
    });
});

const shark = 'Shark! ';
const poopEmoji = '💩';

client.on('message', message => {
  if (message.author.bot) return; //returns doesn't respond to bot or webhook messages.
  const times = Math.floor(Math.random() * 9 + 3);
  if (message.content.toLowerCase().includes('shark')) {
    message.react('🦈');
    message.channel.send(shark.repeat(times)); // do anything here
  }
  if (message.content.toLowerCase().includes('queen')) {
    message.react('👑');
  }
  if (message.content.includes('Doug Ford')) {
    message.react('💩');
  }
  if (message.content.toLowerCase().includes('stunning')) {
    message.react('🥃');
  }
  if (message.content.toLowerCase().includes('bosh')) {
    message.channel.send('https://i.redd.it/xol8lpzdvo471.gif');
  }
  if (message.content.toLowerCase().includes('jail')) {
    message.channel.send('https://c.tenor.com/rlyqxiRiunEAAAAC/tenor.gif');
  }
  if (message.content.startsWith(prefix)) {
    const args = message.content.slice(prefix.length).trim().split(' ');
    const command = args.shift().toLowerCase();
    if (command === 'hype') {
      let compliment = knope.getCompliment(args, 'random');
      message.channel.send(`${compliment}`);
    } else if (command === 'chant') {
      let chant = args[0].charAt(0).toUpperCase() + args[0].slice(1) + '! ';
      message.channel.send(chant.repeat(times + 3));
    } else if (command === 'stunning') {
      message.channel.send(`*Negroni sbagliato oooh*`);
    }
  }

});

// Login to Discord with your client's token
client.login(token).catch(console.error)
client.on('debug', console.log);
client.on("debug", async info => {
  if (info.includes("429")) {
    exec("kill 1")
  }
})