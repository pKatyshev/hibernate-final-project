package ru.javarush.katyshev;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisStringCommands;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;
import org.hibernate.cfg.Environment;
import ru.javarush.katyshev.dao.*;
import ru.javarush.katyshev.entity.*;
import ru.javarush.katyshev.redis.CityCountry;
import ru.javarush.katyshev.redis.Language;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Objects.nonNull;

public class Main {
    private final SessionFactory sessionFactory;
    private final RedisClient redisClient;
    private final ObjectMapper mapper;
    private final CityDAO cityDAO;
    private final CountryDAO countryDAO;

    public Main(String host) {
        sessionFactory = prepareRelationDB(host);
        cityDAO = new CityDAO(sessionFactory);
        countryDAO = new CountryDAO(sessionFactory);

        redisClient = prepareRedisClient(host);
        mapper = new ObjectMapper();
    }

    public static void main(String[] args) {
    }

    public void pushToRedis(List<CityCountry> preparedData) {
        try (StatefulRedisConnection<String, String> connection = redisClient.connect()) {
            RedisStringCommands<String, String> sync = connection.sync();
            for (CityCountry cityCountry : preparedData) {
                try {
                    sync.set(String.valueOf(cityCountry.getId()), mapper.writeValueAsString(cityCountry));
                } catch (JsonProcessingException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public void testRedisData(List<Integer> ids) {
        try (StatefulRedisConnection<String, String> connect = redisClient.connect()) {
            RedisStringCommands<String, String> sync = connect.sync();
            for (Integer id : ids) {
                String value = sync.get(String.valueOf(id));
                try {
                    mapper.readValue(value, CityCountry.class);
                } catch (JsonProcessingException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public void testMySQLData(List<Integer> ids) {
        try (Session session = sessionFactory.getCurrentSession()){
            session.beginTransaction();
            for (Integer id : ids) {
                City city = cityDAO.getById(id);
                Set<CountryLanguage> languages = city.getCountry().getCountryLanguages();
            }
            session.getTransaction().commit();
        }
    }

    public List<CityCountry> transformData(List<City> allCities) {
        return allCities.stream().map(city -> {
            CityCountry res = new CityCountry();
            res.setId(city.getId());
            res.setName(city.getName());
            res.setPopulation(city.getPopulation());
            res.setDistrict(city.getDistrict());

            Country country = city.getCountry();
            res.setAlternativeCountryCode(country.getCode2());
            res.setContinent(country.getContinent());
            res.setCountryCode(country.getCode());
            res.setCountryName(country.getName());
            res.setCountryPopulation(country.getPopulation());
            res.setCountryRegion(country.getRegion());
            res.setCountrySurfaceArea(country.getSurfaceArea());

            Set<Language> languages = country.getCountryLanguages().stream().map(s -> {
                Language language = new Language();
                language.setLanguage(s.getLanguage());
                language.setOfficial(s.getOfficial());
                language.setPercentage(s.getPercentage());
                return language;
            }).collect(Collectors.toSet());

            res.setLanguages(languages);

            return res;
        }).collect(Collectors.toList());
    }

    public List<City> fetchData(Main main) {
        try (Session session = main.sessionFactory.getCurrentSession()) {
            session.getTransaction().begin();
            List<Country> countries = countryDAO.getAll();
            List<City> allCities = new ArrayList<>();

            int totalCount = main.cityDAO.getTotalCount();
            int step = 500;

            for (int i = 0; i < totalCount; i += step) {
                allCities.addAll(cityDAO.getItems(i, step));
            }
            session.getTransaction().commit();
            return allCities;
        }
    }

    private SessionFactory prepareRelationDB(String host) {
        final SessionFactory sessionFactory;
        Properties properties = new Properties();
        properties.put(Environment.DIALECT, "org.hibernate.dialect.MySQL8Dialect");
        properties.put(Environment.DRIVER, "com.p6spy.engine.spy.P6SpyDriver");
        properties.put(Environment.URL, "jdbc:p6spy:mysql://" + host + ":3306/world");
        properties.put(Environment.USER, "root");
        properties.put(Environment.PASS, "root");
        properties.put(Environment.CURRENT_SESSION_CONTEXT_CLASS, "thread");
        properties.put(Environment.HBM2DDL_AUTO, "validate");
        properties.put(Environment.STATEMENT_BATCH_SIZE, "100");

        sessionFactory = new Configuration()
                .addAnnotatedClass(City.class)
                .addAnnotatedClass(Country.class)
                .addAnnotatedClass(CountryLanguage.class)
                .addProperties(properties)
                .buildSessionFactory();
        return sessionFactory;
    }

    public RedisClient prepareRedisClient(String host) {
        RedisClient client = RedisClient.create(RedisURI.create(host, 6379));
        try (StatefulRedisConnection<String, String> connection = client.connect()) {
            System.out.println("=".repeat(18) + "\nConnected to Redis\n" + "=".repeat(18));
        }
        return client;
    }

    public void shutdown() {
        if (nonNull(sessionFactory)) {
            sessionFactory.close();
        }
        if (nonNull(redisClient)) {
            redisClient.shutdown();
        }
    }

    public SessionFactory getSessionFactory() {
        return sessionFactory;
    }
}
