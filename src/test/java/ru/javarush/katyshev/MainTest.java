package ru.javarush.katyshev;

import org.junit.Assert;
import org.junit.Test;
import ru.javarush.katyshev.entity.City;
import ru.javarush.katyshev.redis.CityCountry;

import java.util.List;

import static org.junit.Assert.*;

public class MainTest {
    @Test
    public void testLocalDB(){
        String host = "localhost";
        Main main = new Main(host);
        List<City> allCities = main.fetchData(main);
        List<CityCountry> preparedData = main.transformData(allCities);
        main.pushToRedis(preparedData);

        main.getSessionFactory().getCurrentSession().close();

        List<Integer> ids = List.of(3, 2545, 123, 4, 189, 89, 3458, 1189, 10, 102);

        long startRedis = System.currentTimeMillis();
        main.testRedisData(ids);
        long stopRedis = System.currentTimeMillis();

        long startMysql = System.currentTimeMillis();
        main.testMySQLData(ids);
        long stopMysql = System.currentTimeMillis();

        long redisTime = stopRedis - startRedis;
        long mysqlTime = stopMysql - startMysql;
        System.out.printf("%s:\t%d ms\n", "Redis", redisTime);
        System.out.printf("%s:\t%d ms\n", "MySQL", mysqlTime);

        main.shutdown();

        Assert.assertTrue(redisTime < mysqlTime);
    }

    @Test
    public void testRemoteDB(){
        String host = "192.168.0.245";
        Main main = new Main(host);
        List<City> allCities = main.fetchData(main);
        List<CityCountry> preparedData = main.transformData(allCities);
        main.pushToRedis(preparedData);

        main.getSessionFactory().getCurrentSession().close();

        List<Integer> ids = List.of(3, 2545, 123, 4, 189, 89, 3458, 1189, 10, 102);

        long startRedis = System.currentTimeMillis();
        main.testRedisData(ids);
        long stopRedis = System.currentTimeMillis();

        long startMysql = System.currentTimeMillis();
        main.testMySQLData(ids);
        long stopMysql = System.currentTimeMillis();

        long redisTime = stopRedis - startRedis;
        long mysqlTime = stopMysql - startMysql;
        System.out.printf("%s:\t%d ms\n", "Redis", redisTime);
        System.out.printf("%s:\t%d ms\n", "MySQL", mysqlTime);

        main.shutdown();

        Assert.assertTrue(redisTime < mysqlTime);
    }
}