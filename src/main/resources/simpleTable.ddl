CREATE DATABASE options;

USE options;

CREATE TABLE simple_example (
    underlying          varchar(10),
    underlying_last     DOUBLE
);

CREATE TABLE historical_options (
    underlying			varchar(30),
    underlying_last			varchar(30),
    exchange			varchar(30),
    optionroot			varchar(30),
    optionext			varchar(30),
    type			varchar(30),
    expiration			varchar(30),
    quotedate			varchar(30),
    strike			varchar(30),
    last			varchar(30),
    bid			varchar(30),
    ask			varchar(30),
    volume			varchar(30),
    openinterest			varchar(30),
    impliedvol			varchar(30),
    delta			varchar(30),
    gamma			varchar(30),
    theta			varchar(30),
    vega			varchar(30),
    optionalias			varchar(30),
    IVBid			varchar(30),
    IVAsk           varchar(30)
);