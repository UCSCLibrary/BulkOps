# BulkOps: Bulk Operations for Hyrax
This is a plugin that adds bulk ingest and bulk update functionality to Hyrax based repositories

## Prerequisites
- This gem currently requires users to also use UCSC's ScoobySnacks gem manage their repository's metadata schema. We plan to make this optional in the future.

## Installation
Add the following to your gem file:

```gem 'bulk_ops'```

- run the following commands from your rails app root directory:

```
bundle install
bundle exec rails generate bulk_ops:install
```
