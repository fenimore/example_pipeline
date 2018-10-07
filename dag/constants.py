WORKWEEK = {
    0: True,
    1: True,
    2: True,
    3: True,
    4: True,
    5: False,
    6: False,
}

SEASON = {
    1: "winter",
    2: "winter",
    3: "winter",
    4: "spring",
    5: "spring",
    6: "spring",
    7: "summer",
    8: "summer",
    9: "summer",
    10: "fall",
    11: "fall",
    12: "fall",
}

ZODIAC = {
    0: 'Dragon',
    1: 'Snake',
    2: 'Horse',
    3: 'sheep',
    4: 'Monkey',
    5: 'Rooster',
    6: 'Dog',
    7: 'Pig',
    8: 'Rat',
    9: 'Ox',
    10: 'Tiger',
    11: 'Hare',
}

SIGNS = [
    "capricorn",
    "aquarius",
    "pisces",
    "aries",
    "taurus",
    "gemini",
    "cancer",
    "leo",
    "virgo",
    "libra",
    "scorpio",
    "sagittarius",
]


def _horoscope(day, month):
    if month == 1:
        if day < 20:
            return "capricorn"
        else:
            return "aquarius"
    elif month == 2:
        if day < 19:
            return "aquarius"
        else:
            return "pisces"
    elif month == 3:
        if day < 21:
            return "pisces"
        else:
            return "aries"
    elif month == 4:
        if day < 20:
            return "aries"
        else:
            return "taurus"
    elif month == 5:
        if day < 21:
            return "taurus"
        else:
            return "gemini"
    elif month == 6:
        if day < 21:
            return "gemini"
        else:
            return "cancer"
    elif month == 7:
        if day < 23:
            return "cancer"
        else:
            return "leo"
    elif month == 8:
        if day < 23:
            return "leo"
        else:
            return "virgo"
    elif month == 9:
        if day < 20:
            return "virgo"
        else:
            return "libra"
    elif month == 10:
        if day < 20:
            return "libra"
        else:
            return "scorpio"
    elif month == 11:
        if day < 20:
            return "scorpio"
        else:
            return "sagittarius"
    elif month == 12:
        if day < 22:
            return "sagittarius"
        else:
            return "capricorn"
