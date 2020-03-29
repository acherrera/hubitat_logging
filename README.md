# Hubitat Logging

This is some code to keep track of log data for the hubitat system. The is done in a pretty lazy way - sorry, I just
wanted to something to work and this seems to work. 

## Basic flow. 

hubitat => router => node-red => log_file => python / pandas => postgres => grafana

If this were better, or I cared more, python would grab the data straight from the websocket and put it into the
database. I did attempt to do this once but found that it missed quite a bit of data compared to node-red. My guess is
that this has to do with the GIL on Python and message coming in all at once. While this is a problem I could fix, I
did not want to start messing around with multiprocessing when the current system worked. 

### node-red

The node-red flow is in the ./node-red-flow/ directory and it's stupid simple. Just add data to a file

### Python

The python program is just simple script located in the root directory - `./postgres_transfer.py`. It manages the
starting / stopping of the node-red program. The reason for this is that if the router stops working, node-red will stop
working and is unable to fix itself. So, I have python stop the program and restart it. Is it hacky? Yeah. Does it work?
Yeah. 

### Postgres

You may notice I don't have any models or sql scripts to make models. That's because I didn't write any. Pandas pulls
the data and add it to the database however it feels like doing it. Turns out, this is good enough for me. This also has
the advantage of being able to store an arbitrary number of sensors with out preconfigured models. I attempted to this
manually as well and decided pandas was good enough. 

### Grafana

Oh man... I thought this was going to be WAY harder than it turned out to be. Download it, set it up, make graphs and
you're good to go. Because pandas is used to "manage" the models, the queries can get a little ugly as you must CAST
types on nearly every call and "open" and "close" have to be converted to 0 and 1 in order to plot. But.... it works. So
I left it
