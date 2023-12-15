package ru.javarush.katyshev.dao;

import org.hibernate.SessionFactory;
import org.hibernate.query.Query;
import ru.javarush.katyshev.entity.Country;

import java.util.List;

public class CountryDAO {
    private final SessionFactory sessionFactory;

    public CountryDAO(SessionFactory sessionFactory) {
        this.sessionFactory = sessionFactory;
    }

    public List<Country> getAll() {
        Query<Country> query = sessionFactory.getCurrentSession()
                .createQuery("select c from Country c join fetch c.countryLanguages", Country.class);
        return query.list();
    }
}
