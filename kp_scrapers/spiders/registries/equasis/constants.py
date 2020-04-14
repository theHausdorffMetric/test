"""Equasis settings and constants.

We need a large set of credentials to loop through because Equasis bans our users pretty quickly.
Please use the dev credentials when not running on production to preserve our prod users' quota.

Now we found that we are banned almost immediately with temporary e-mails, so we change our
strategy:
    - Using realistic e-mails with password saved in `_mail_pwd` field
    - Using alias trick to create multiple Equasis account

Note that don't create Equasis account with alias trick in a short period time, this could get all
the alias account banned.


NOTE the process coule be automated later (https://temp-mail.org/en/api/) but
note we still need to fill Equasis form, wich include a visual code.
So in the meatime : https://uinames.com/

"""

DEV_USERS = [
    {'login': 'honuzireko@vpn33.top', 'password': 'honuzireko'},
    {'login': 'foyejod@1rentcar.top', 'password': 'foyejod'},
]

# `_mail_pwd` is provided here mainly for mailbox monitoring purposes,
# in the event Equasis sends emails upon banning logins
PROD_USERS = [
    {
        'login': 'a.gibson08+charterer@protonmail.ch',
        'password': 'fNrAF9TP',
        '_mail_pwd': 'AAwQk32LDznzgFp4',
    },  # noqa
    {'login': 'a.logarius@inbox.lv', 'password': 'A4r06B71', '_mail_password': 'thefullmonty'},
    {
        'login': 'charterer.guy0708+1123@protonmail.com',
        'password': 'mZloUzZP',
        '_mail_pwd': 'password',
    },  # noqa
    {'login': 'charterer.guy89+a@gmail.com', 'password': 'deJYNJcn', '_mail_pwd': 'bigdata123'},
    {'login': 'd.alva@tutanota.com', 'password': 'a1VmU7ld', '_mail_password': 'allhailkpler@Q'},
    {'login': 'e.ornstein@gmx.de', 'password': 'e83xBNED', '_mail_password': 'praiseit'},
    {'login': 'h.gehrman@gmx.ch', 'password': 'EH2fihOe', '_mail_password': 'destorigin123'},
    {'login': 'haight0708+marine@protonmail.com', 'password': 'TKTBhNYC', '_mail_pwd': 'coittower'},
    {'login': 'kang.bao@yandex.com', 'password': 'auVT9iK1', '_mail_password': 'KplerKpler+123'},
    {
        'login': 'kperhabkli@tutanota.com',
        'password': 'YRpLDUaV',
        '_mail_password': 'KplerKpler+123',
    },  # noqa
    {
        'login': 'lisa.marks+125@protonmail.ch',
        'password': 'PkNIoPVF',
        '_mail_pwd': '8HhJQQus3BqyNxMf',
    },  # noqa
    {
        'login': 'medfordor+1123@protonmail.ch',
        'password': 'glmR6YhM',
        '_mail_pwd': '3vvmRjWBYjdnTKNs',
    },  # noqa
    {'login': 'mikhail+5@mailfence.com', 'password': 'aI1RK4eA', '_mail_pwd': 'crazymouse415'},
    {'login': 'mmoller+1@mailfence.com', 'password': 't3Sl4Z5s', '_mail_pwd': 'UkWTZRUTwTw6fzHE'},
    {'login': 'nezegopa@voltaer.com', 'password': 'n82PVB7l', '_mail_password': None},
    {'login': 'okayxiaoma@protonmail.com', 'password': '7pYmYwUP', '_mail_password': 'kpler+123'},
    {'login': 'p.gascoigne@inbox.lv', 'password': 'k4aJEi89', '_mail_password': 'firsttime123'},
    {'login': 'wallawalla08+jlil@protonmail.com', 'password': 'Z77frcNy', '_mail_pwd': 'nBr76IUn'},
    {'login': 'wangh_equal@protonmail.com', 'password': 'vishgiVn', '_mail_password': 'kplerkpler'},
    {'login': 'xmjxmjaaa+chr@gmail.com', 'password': 'avKKepLq', '_mail_pwd': 'ASDjk123'},
    {'login': 'zswhdxmj+anklih@gmail.com', 'password': 'QgKMGyf0', '_mail_password': None},
]

_OLD_PROD_USERS = [
    # TODO create new alt logins
    {'login': 'addison.patel@mailfence.com', 'password': 'ihz5fg2w', '_mail_pwd': 'orangecat288'},
    {'login': 'deann.fox@mailfence.com', 'password': 'HJadcMwf', '_mail_pwd': 'sBUWCPks6vAAHsp8'},
    {'login': 'elizabeth.smith@mailfence.com', 'password': 'Zj2kzYBJ', '_mail_pwd': 'silverfrog63'},
    {'login': 'gsartor91@gmx.com', 'password': '7XWx4MeI', '_mail_pwd': 'uU3ns4DH4uaQeuEj'},
    {'login': 'charterer.guy20180806@gmail.com', 'password': 'password', '_mail_pwd': 'pa55word!'},
    {'login': 'aberdeenwa08@gmail.com', 'password': 'Qy8rEw9X', '_mail_pwd': 'Fzg9x2JAErjZeMq6'},
    # FIXME doesn't support alias
    {'login': 'f.sandal@tutanota.de', 'password': 'PHTSrzZK', '_mail_pwd': 'exuGVGHu8Tr7j9e3'},
    {'login': 'filmore1993@tuta.io', 'password': 'BeXvKS7B', '_mail_pwd': 'bigpanda441'},
    {'login': 'spokanian@tutanota.com', 'password': 'H74GZ2aW', '_mail_pwd': 'ezNFRQN5h2u2ZSmh'},
    {'login': 'r.pierce@tuta.io', 'password': 'UPQIPCBv', '_mail_pwd': '8x73b9BtVfhHgVQr'},
    {'login': 'mig.robin@tutanota.de', 'password': '0aWTF3vw', '_mail_pwd': '3ge7aCu5jNjB3NGM'},
    {'login': 'l.hautala@keemail.me', 'password': 'XoQLLSyN', '_mail_pwd': 'tQrZaDv9x4wDtv9S'},
    {'login': 'h.wheeler@keemail.me', 'password': 'jFMyTWz5', '_mail_pwd': 'raK7nQbjZKeaPRH5'},
    {'login': 'k.reid@tuta.io', 'password': 'uAnK2uyh', '_mail_pwd': 'haC926pGwWTNVCfy'},
]

# Broader categories than vessel types, needed for search on Equasis interface
VESSEL_CATEGORIES = [
    {'code': '5', 'name': 'Bulk Carriers', 'metrics': 0},
    {'code': '6', 'name': 'Oil and Chemical Tankers', 'metrics': 0},
    {'code': '7', 'name': 'Gas Tankers', 'metrics': 0},
]

# ban appears to be linked to download limit per day
BANNED_TIME = 1 * 24 * 3600

# average interval between requests (in seconds)
AVG_DELAY = 40

# when identifying Equasis vessels from newbuilds cache, take ±10 % of DWT
DWT_APPROXIMATION = 0.10

# default page settings
DEFAULT_MIN_PAGE = 1
DEFAULT_MAX_PAGE = 1000000

# search setting
SEARCH_QUOTA = 10
