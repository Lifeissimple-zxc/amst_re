# Amst RE

*Amst RE* (Amsterdam Real Estate) is a framework that automates house hunting in the Netherlands. It does so by fetching data from [Funda](https://www.funda.nl/) and [Pararius](https://www.pararius.com). It enables to react to new listings faster thus increasing one's chances for a successful bid.

It has Amst because it was initially developed it for Amstedam specifically, but it can monitor any city as of today.

The process is simple and consists of the following steps:

- Read search URLs from a Gsheet.
- Check what listings are available.
- Compares listings with data in DB to filter out what is net new.
- Send alerts on new listings to [Telegram](https://web.telegram.org/).