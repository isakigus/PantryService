
Coding Challeng
================

Overview
--------
You are asked to complete the coding challenge which is described in detail in
the following paragraphs. Kindly note the following requirements:

* Use the Python 2.7+ programming language to complete the challenge.
* Do not use any external libraries, only the pure Python 2.7+ core modules.
  There is one exception to this rule: wsgiservice. wsgiservice is a lean
  Python WSGI framework for very easy creation of REST services.
* Structure your code into several Python modules and services and add a simple
  unit test.
* Comment your code wisely.

It should not take you more than 4 hours to complete the challenge.


Challenge
---------

The challenge consists in writing a piece of software that allows data to be
processed, stored in a persistent manner, and retrieved by the caller of the
API. To simplify things we assume the incoming data is always of textual
nature. Moreover, the caller of the API provides a unique key with every
request to the software component.

A simple example implementation that is done with Python classes is provided
below::


    class KeyValueStore(object):
        """Simplistic key-value store that uses main memory as the storage
        backend."""

        def __init__(self):
            self._store = {}

        def set(self, key, value):
            """Store the provided `value` for the provided `key`."""
            self._store[key] = value

        def get(self, key):
            """Return the stored value for the provided `key` or `None` if no
            such key is present."""
            ret = self._store.get(key)
            return ret


    class LanguageDetector(object):
        """Dummy language detector which makes use of a language specific rule
        set."""

        def __init__(self):
            pass

        def process(self, text):
            """Detect the language for the provided `text`. Returns a
            two-letter language code (ISO 639-1)."""
            if u'z\xfcrich' in text.lower():
                ret = 'de'
            else:
                ret = 'en'
            return ret


    class TextualKeyValueStore(object):
        """Key-value store which detects the language of incoming data before
        storing it."""

        def __init__(self):
            self.ld = LanguageDetector()
            self.store = KeyValueStore()

        def set(self, key, value):
            lang = self.ld.process(value)
            self.store.set(key, {'lang': lang, 'value': value})

        def get(self, key):
            ret = self.store.get(key)
            return ret


Obviously, a more elaborate data processing module is to be implemented. In
particular a system is needed that takes into account the fact that servers
have tiered storage systems (e.g. main memory, SSDs, disks) and that
processing and storage capacity resides on multiple machines within a data
center (e.g. 3 nodes with equal processing and storage capacity). It is up to
you to decide on an architecture for the system to be implemented (classes,
modules, services, etc.). Also, how would you deploy your code to production?
What is it that you do if an update to the code base is required?

In general, it is better to have a simple working solution than no solution at
all. However, an implementation which is too simplistic is not ideal either.
In that sense you must compromise on the complexity and completeness of your
approach, given the restricted time frame of 4 hours.


Examples
````````
To facilitate the implementation the following examples can be used:

* Sample keys:

  * F51DE427-6694-490C-A90B-055B156052EC
  * 47CB9661-520D-4674-B79B-B0A1FD3805F1
  * C3DABBD8-E411-4477-8711-A0E185717B09
  * B03010F2-BB70-4769-BA7A-57F4620F508B


* Sample Texts:
  * Continental does not have an office in Zürich.

  * The Burke County location of Continental is a division of a larger
    corporation with sales of around $46 billion in 2013. Continental is among
    the leading automotive suppliers worldwide. As a supplier of brake
    systems, systems and components for powertrains and chassis,
    instrumentation, infotainment solutions, vehicle electronics, tires, and
    technical elastomers, Continental contributes to enhanced driving safety
    and global climate protection.  Continental is also an expert partner in
    networked automobile communication.  Continental currently employs around
    178,000 people in 49 countries. The two Morganton facilities are part of
    the Vehicle Dynamics Business Unit, which is one of four Business Units
    within Continental.

  * “We are proud of our company, our products and our facilities,” said Naomi
    Cole, senior human resource manager for Continental Automotive Systems in
    Morganton. “Each of us has an important job to do, and ultimately the
    success of our company depends upon how well we individually and
    collectively perform our jobs and ultimately satisfy our customers. We
    seek employees who have the ability to contribute toward our success in a
    meaningful way. It is the success of our company as a profitable business
    that will increase our job security, opportunities for personal growth,
    and support Burke and the surrounding counties.”

  * The report reveals that Bridgestone, Michelin, Goodyear, Pirelli and
    Continental are few of the dominant tyre manufacturers in the UAE,
    accounting for a substantial share in the country's tyre market. These
    leading players are constantly growing due to their well-established
    supply chain network, comprising exclusive distributorships and local
    dealers.









