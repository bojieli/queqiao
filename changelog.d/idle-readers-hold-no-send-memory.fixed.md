A client with a shared send budget (the iOS and Android profiles) no longer
lets idle application sockets hold that budget. Each flow used to reserve a
whole chunk before waiting to read from its application, so on iOS 192 idle
keep-alive connections held all 3 MiB and a request behind them was never
read, nor was an EOF on a later socket; nothing released them. A flow now
waits on its application with only a 1 KiB private buffer, reserves shared
memory once it has bytes in hand, and fills the rest of the chunk from what
the application has already written (#169).
