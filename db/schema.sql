-- table declarations :
create table books (
    publisher_id varchar(128) not null,
    description VARCHAR(30000),
    number_of_sections int not null,
    language_code CHAR(2),
    isbn varchar(128) not null,
    publication_date DATE not null,
    title VARCHAR(255) not null,
    cover_url VARCHAR(255)
  );
create table publishers (
    name VARCHAR(128) not null,
    ebook_discount int not null,
    implements_agency_pricing_model boolean not null,
    id int not null,
    country_code VARCHAR(4)
  );
create table user_clubcards (
    clubcard_id VARCHAR(20) not null,
    user_id int not null
  );
create table currency_rates (
    rate decimal(20,16) not null,
    from_currency VARCHAR(5) not null,
    to_currency VARCHAR(5) not null
  );
create table contributors (
    full_name VARCHAR(256) not null,
    last_name VARCHAR(256),
    first_name VARCHAR(256),
    guid VARCHAR(256),
    url VARCHAR(256),
    image_url VARCHAR(256),
    id int not null
  );
create table contributor_roles (
    role int not null,
    isbn varchar(128) not null,
    contributor_id int not null
  );
create table genres (
    name varchar(128),
    bisac_code VARCHAR(8),
    id int not null,
    parent_id int
  );
create table book_genres (
    isbn VARCHAR(13) not null,
    genre_id int not null
  );
