CREATE TABLE dev.user
(
   id  serial primary key,
   name text not null,
   email text not null,
   dob date not null,
   address text not null,
   country text not null,
   state text not null,
   city text not null,
   account_id int not null,
   add_by text,
   upd_by text,
   add_on timestamp with time zone default current_timestamp,
   upd_on timestamp with time zone default current_timestamp
);

CREATE TABLE dev.truck
(
   id  serial primary key,
   brand text not null,
   make text not null,
   model text not null,
   address text not null,
   country text not null,
   state text not null,
   city text not null,
   registration text not null,
   add_by text,
   upd_by text,
   add_on timestamp with time zone default current_timestamp,
   upd_on timestamp with time zone default current_timestamp
);

CREATE TABLE dev.bank
(
   id  serial primary key,
   type text not null,
   active boolean not null,
   balance int not null,
   transact int,
   card int,
   user_id int,
   loan int,
   account_id int not null,
   add_by text,
   upd_by text,
   add_on timestamp with time zone default current_timestamp,
   upd_on timestamp with time zone default current_timestamp
);


CREATE TABLE dev.garage
(
   id  serial primary key,
   country text not null,
   state text not null,
   city date not null,
   address text not null,
   size text not null,
   class text not null,
   active text not null,
   status text not null,
   add_by text,
   upd_by text,
   add_on timestamp with time zone default current_timestamp,
   upd_on timestamp with time zone default current_timestamp
);

CREATE TABLE dev.route
(
   id  serial primary key,
   from_loc text not null,
   to_loc text not null,
   has_toll boolean not null,
   address text not null,
   size text not null,
   class text not null,
   active text not null,
   status text not null,
   add_by text,
   upd_by text,
   add_on timestamp with time zone default current_timestamp,
   upd_on timestamp with time zone default current_timestamp
);                          

CREATE TABLE dev.region
(
   id  serial primary key,
   country text not null,
   state text not null,
   city text not null,
   add_by text,
   upd_by text,
   add_on timestamp with time zone default current_timestamp,
   upd_on timestamp with time zone default current_timestamp
);                          

CREATE TABLE dev.route
(
   id  serial primary key,
   from_loc text not null,
   to_loc text not null,
   amount int,
   mode text,
   status text,
   add_by text,
   upd_by text,
   add_on timestamp with time zone default current_timestamp,
   upd_on timestamp with time zone default current_timestamp
);                          

CREATE TABLE dev.loan
(
   id  serial primary key,
   account_id text not null,
   user_id text not null,
   add_by text,
   upd_by text,
   add_on timestamp with time zone default current_timestamp,
   upd_on timestamp with time zone default current_timestamp
);     

CREATE TABLE dev.trip
(
   id  serial primary key,
   user_id text not null,
   trip_id text not null,
   add_by text,
   upd_by text,
   add_on timestamp with time zone default current_timestamp,
   upd_on timestamp with time zone default current_timestamp
);     
