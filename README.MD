Create data sample from a SQL database, with relation.

This is good to extract data from all tables, but filtered by a query.

## Use case

* dev setup => you can have a small DB that represents the real DB
* ci setup

## Usage

```
python sample.py --json --sql --csv my_app.conf
```

## Configuration

"my_app.conf", HOCON format:

```
sampler {

  mysql {
    host: "localhost",
    port: 3306,
    user: "root",
    password: "root",
    db: "epf"
  }

  filters {
    artists {
      query: "select artist_id from epf_artist where name in {names}",
      args: {
        names: [ "Iggy Pop", "Rihanna", "Rihana", "David Guetta", "Paul Binkley" ]
      }
    }
  }

  tables {
    epf_artist {
      where: "artist_id IN {artists}"
    }

    epf_artist_match {
    }


    epf_collection_translation: {
      where : "collection_id IN (SELECT collection_id FROM epf_artist_collection WHERE artist_id IN {artists})"
    }

  }
}

```
