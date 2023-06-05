## Scrapy SQL pipeline example & stats collection

Shows how items scraped off an example news site are saved in a sql database via a [scrapy pipeline](https://docs.scrapy.org/en/latest/topics/item-pipeline.html).

Including some [custom stats](https://docs.scrapy.org/en/latest/topics/stats.html), as well as collection of potential sql errors. Pushing the entire stats of the spiders crawl into a sql database with the [spiders closed method](https://docs.scrapy.org/en/latest/topics/spiders.html#scrapy.Spider.closed).

The stats are returned as a dictionary. Once thats converted into a json, it is then dumped as json into the databank using its json compatability.

The [mariadb documentation](https://mariadb.com/resources/blog/using-json-in-mariadb/) is essential reading regarding the combination of sql and json. Other sql databases will have their own comparable functunality and documentation on the subject.
