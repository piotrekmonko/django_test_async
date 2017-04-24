Django Parallel Tests
=====================

Package allows running Django 1.6 test suites in parallel, greatly reducing time
they take to run.

Requirements:

Install:


    pip install django_test_async

Add to Django:


    INSTALLED_APPS += ('django_test_async',)

Run:


    $ time python manage.py test_async --processes 4


----

Test async uses own test runner `AsyncRunner` which is a subclass of standard
Django `DiscoverRunner` - modify if needed or include in your own runner.

Important:

- 'test_async' command substitutes settings.DATABASES engines
    with 'django.db.backends.sqlite3'
- some tests might not work in parallel
    TODO: add a conditional context manager there
- '--processes' parameter controls how many threads will be used,
    defaults to the number of CPUs, some experimantation needed for best result,
    #CPU * 1.2 works fine, test using eg. bash time


