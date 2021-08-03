-- QUESTÃO 1

DO
$$
    DECLARE
        v_query   RECORD;
        v_command VARCHAR;
    BEGIN
        FOR v_query IN (
            SELECT EXTRACT(YEAR FROM (so.date)) AS ano
            FROM sale_old so
            GROUP BY 1
            ORDER BY 1
        )
            LOOP
                v_command = FORMAT(
                        'CREATE TABLE IF NOT EXISTS sale_%s PARTITION of sale for VALUES from (%s) to (%s);',
                        v_query.ano,
                        QUOTE_LITERAL(CONCAT(v_query.ano::VARCHAR, '-01-01 00:00:00.000')),
                        QUOTE_LITERAL(CONCAT(v_query.ano::VARCHAR, '-12-31 23:59:59.999'))
                    );

                RAISE NOTICE '%', v_command;
                EXECUTE v_command;
            END LOOP;
    END;
$$;


INSERT INTO sale (id,
                  id_customer,
                  id_branch,
                  id_employee,
                  date, created_at,
                  modified_at,
                  active)
SELECT id,
       id_customer,
       id_branch,
       id_employee,
       date,
       created_at,
       modified_at,
       active
FROM sale_old so;

CREATE OR REPLACE FUNCTION f_sale() RETURNS TRIGGER AS
$$
DECLARE
    v_command VARCHAR;
BEGIN

    v_command = FORMAT(
            'CREATE TABLE IF NOT EXISTS sale_%s PARTITION of sale for VALUES from (%s) to (%s);',
            EXTRACT(YEAR FROM new.data),
            QUOTE_LITERAL(CONCAT(EXTRACT(YEAR FROM new.data)::VARCHAR, '-01-01 00:00:00.000')),
            QUOTE_LITERAL(CONCAT(EXTRACT(YEAR FROM new.data)::VARCHAR, '-12-31 23:59:59.999'))
        );

    RAISE NOTICE '%', v_command;
    EXECUTE v_command;

    RETURN new;
END;
$$
    LANGUAGE plpgsql;

CREATE TRIGGER tg_sale
    BEFORE INSERT
    ON sale
    FOR EACH ROW
EXECUTE FUNCTION f_sale();



-- QUESTÃO 2

SELECT *
FROM crosstab(
             $$
                SELECT  pg.name                         AS product_group,
                        TO_CHAR(s.date, 'Month')        AS month,
                        SUM(p.sale_price * si.quantity) AS sale
                FROM sale s
                    INNER JOIN sale_item si ON s.id = si.id_sale AND EXTRACT(YEAR FROM s.date) = 2021
                    INNER JOIN product p ON p.id = si.id_product
                    INNER JOIN product_group pg ON pg.id = p.id_product_group
                GROUP BY 1, 2
        $$,
             $$
                SELECT
                    TO_CHAR(
                            TO_DATE(a::TEXT, 'MM'), 'Month'
                        ) AS month
                FROM
                    GENERATE_SERIES(1, 12) a
            $$
         ) AS (
               product_group VARCHAR,
               "January" NUMERIC,
               "February" NUMERIC,
               "March" NUMERIC,
               "April" NUMERIC,
               "May" NUMERIC,
               "June" NUMERIC,
               "July" NUMERIC,
               "August" NUMERIC,
               "September" NUMERIC,
               "October" NUMERIC,
               "November" NUMERIC,
               "December" NUMERIC
    );



-- QUESTÃO 3

CREATE EXTENSION IF NOT EXISTS "tablefunc";

SELECT *
FROM crosstab(
             $$
                SELECT  d.name                        AS district,
                        z.name                        AS zone,
                        COUNT(DISTINCT s.id_customer) AS amount_customers
                FROM sale s
                    INNER JOIN customer c ON s.id_customer = c.id
                    INNER JOIN district d ON c.id_district = d.id
                    INNER JOIN zone z ON z.id = d.id_zone
                GROUP BY 1, 2
        $$,
             $$
                SELECT zone.name
                FROM zone
            $$
         ) AS (
               product_group VARCHAR,
               "Norte" NUMERIC,
               "Sul" NUMERIC,
               "Leste" NUMERIC,
               "Oeste" NUMERIC
    );



-- QUESTÃO 4    imcomplete

ALTER TABLE sale_item ADD COLUMN unit_price NUMERIC(17, 2);


UPDATE sale_item AS si
SET unit_price = (
    SELECT p.sale_price
    FROM product p
    WHERE p.id = si.id_product
)
WHERE TRUE;


-- QUESTÃO 5    imcomplete

ALTER TABLE sale_item ADD COLUMN total_price NUMERIC(17, 2);


UPDATE sale_item AS si
SET total_price = (
    SELECT p.sale_price
    FROM product p
    WHERE p.id = si.id_product
)
WHERE TRUE;



-- QUESTÃO 6    imcomplete

CREATE DATABASE crime;

CREATE TABLE arma
    (
        id            SERIAL,
        numero_serie  VARCHAR(104) NOT NULL,
        descricao     VARCHAR(256) NOT NULL,
        tipo          VARCHAR(1)   NOT NULL,
        ativo         BOOLEAN      NOT NULL,
        criado_em     TIMESTAMP    NOT NULL,
        modificado_em TIMESTAMP    NOT NULL,
        CONSTRAINT pk_arma
            PRIMARY KEY (id)
    );

CREATE TABLE tipo_crime
    (
        id                  SERIAL,
        nome                VARCHAR(104) NOT NULL,
        tempo_minimo_prisao SMALLINT,
        tempo_maximo_prisao SMALLINT,
        tempo_prescricao    SMALLINT,
        ativo               BOOLEAN      NOT NULL,
        criado_em           TIMESTAMP,
        modificado_em       TIMESTAMP    NOT NULL,
        CONSTRAINT pk_tipo_crime
            PRIMARY KEY (id)
    );

CREATE TABLE crime
    (
        id            SERIAL,
        id_tipo_crime INTEGER      NOT NULL,
        data          TIMESTAMP    NOT NULL,
        local         VARCHAR(256) NOT NULL,
        observacao    TEXT         NOT NULL,
        ativo         BOOLEAN      NOT NULL,
        criado_em     TIMESTAMP    NOT NULL,
        modificado_em TIMESTAMP    NOT NULL,
        CONSTRAINT pk_crime
            PRIMARY KEY (id)
    );

CREATE TABLE crime_arma
    (
        id            SERIAL,
        id_crime      INTEGER   NOT NULL,
        id_arma       INTEGER   NOT NULL,
        ativo         BOOLEAN   NOT NULL,
        criado_em     TIMESTAMP NOT NULL,
        modificado_em TIMESTAMP NOT NULL,
        CONSTRAINT pk_crime_arma
            PRIMARY KEY (id),
        CONSTRAINT ak_crime_arma
            UNIQUE (id_arma, id_crime)
    );

DROP TABLE pessoa;
CREATE TABLE pessoa
    (
        id              SERIAL,
        nome            VARCHAR(104) NOT NULL,
        cpf             VARCHAR(11)  NOT NULL,
        telefone        VARCHAR(11)  NOT NULL,
        data_nascimento DATE         NOT NULL,
        endereco        VARCHAR(256) NOT NULL,
        ativo           BOOLEAN      NOT NULL,
        criado_em       TIMESTAMP    NOT NULL,
        modificado_em   TIMESTAMP    NOT NULL,
        CONSTRAINT pk_pessoa
            PRIMARY KEY (id),
        CONSTRAINT ak_pessoa_cpf
            UNIQUE (cpf)
    );

CREATE TABLE crime_pessoa
    (
        id            SERIAL,
        id_pessoa     INTEGER    NOT NULL,
        id_crime      INTEGER    NOT NULL,
        tipo          VARCHAR(1) NOT NULL,
        ativo         BOOLEAN    NOT NULL,
        criado_em     TIMESTAMP  NOT NULL,
        modificado_em TIMESTAMP  NOT NULL,
        CONSTRAINT pk_crime_pessoa
            PRIMARY KEY (id),
        CONSTRAINT ak_pessoa_crieme
            UNIQUE (id_pessoa, id_crime)
    );


ALTER TABLE crime ADD CONSTRAINT fk_crime_tipo_crime FOREIGN KEY (id_tipo_crime) REFERENCES tipo_crime (id);
ALTER TABLE crime_arma ADD CONSTRAINT fk_crime_arma_arma FOREIGN KEY (id_arma) REFERENCES arma (id);
ALTER TABLE crime_arma ADD CONSTRAINT fk_crime_arma_crime FOREIGN KEY (id_crime) REFERENCES crime (id);
ALTER TABLE crime_pessoa ADD CONSTRAINT fk_crime_pessoa_pessoa FOREIGN KEY (id_pessoa) REFERENCES pessoa (id);
ALTER TABLE crime_pessoa ADD CONSTRAINT fk_crime_pessoa_crime FOREIGN KEY (id_crime)REFERENCES crime (id);


-- QUESTÃO 7