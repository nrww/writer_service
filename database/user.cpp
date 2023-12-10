#include "user.h"
#include "database.h"
#include "../config/config.h"

#include <Poco/Data/MySQL/Connector.h>
#include <Poco/Data/MySQL/MySQLException.h>
#include <Poco/Data/SessionFactory.h>
#include <Poco/Data/RecordSet.h>
#include <Poco/JSON/Parser.h>
#include <Poco/Dynamic/Var.h>
#include <cppkafka/cppkafka.h>

#include <sstream>
#include <exception>
#include <algorithm>
#include <future>
#include <mutex>

using namespace Poco::Data::Keywords;
using Poco::Data::Session;
using Poco::Data::Statement;

namespace database
{

    void User::init()
    {
        try
        {

            Poco::Data::Session session = database::Database::get().create_session();

            long last_shard_id = 0;
            for (auto &hint : Database::get_all_hints())
            {
                Statement create_stmt(session);

                create_stmt << "CREATE TABLE IF NOT EXISTS `user` ("
                        << "`user_id` INT NOT NULL AUTO_INCREMENT,"
                        << "`first_name` VARCHAR(256) NOT NULL,"
                        << "`last_name` VARCHAR(256) NOT NULL,"
                        << "`login` VARCHAR(256) NOT NULL,"
                        << "`password` VARCHAR(256) NOT NULL,"
                        << "`email` VARCHAR(256) NULL,"
                        << "`phone` VARCHAR(15) NULL,"
                        << "PRIMARY KEY (`user_id`),KEY `fn` (`first_name`),KEY `ln` (`last_name`));"
                        << hint,
                        now;
                create_stmt.execute();

                Poco::Data::Statement select(session);
                long temp_id = 0;
                std::string sql = "SELECT `user_id` FROM `user` ORDER BY `user_id` DESC LIMIT 0, 1 ";
                sql += hint;
                select  << sql,
                    into(temp_id),
                    range(0, 1); //  iterate over result set one row at a time

                select.execute();
                if (last_shard_id < temp_id)
                {
                    last_shard_id = temp_id;
                }
            }
            _last_id = last_shard_id;
            
            
        }
        catch (Poco::Data::MySQL::ConnectionException &e)
        {
            std::cout << e.displayText() << std::endl;
        }
        catch (Poco::Data::MySQL::StatementException &e)
        {
            std::cout << e.displayText() << std::endl;
        }
    }



    Poco::JSON::Object::Ptr User::toJSON() const
    {
        Poco::JSON::Object::Ptr root = new Poco::JSON::Object();

        root->set("user_id", _id);
        root->set("first_name", _first_name);
        root->set("last_name", _last_name);
        root->set("email", _email);
        root->set("phone", _phone);
        root->set("login", _login);
        root->set("password", _password);

        return root;
    }

    User User::fromJSON(const std::string &str)
    {
        User user;
        Poco::JSON::Parser parser;
        Poco::Dynamic::Var result = parser.parse(str);
        Poco::JSON::Object::Ptr object = result.extract<Poco::JSON::Object::Ptr>();

        user.id() = object->getValue<long>("user_id");
        user.first_name() = object->getValue<std::string>("first_name");
        user.last_name() = object->getValue<std::string>("last_name");
        user.email() = object->getValue<std::string>("email");
        user.phone() = object->getValue<std::string>("phone");
        user.login() = object->getValue<std::string>("login");
        user.password() = object->getValue<std::string>("password");

        return user;
    }
    //not working
    std::optional<long> User::auth([[maybe_unused]] std::string &login, [[maybe_unused]] std::string &password)
    {
        /*std::vector<long> result;

        std::vector<std::string> hints = database::Database::get_all_hints();
        std::vector<std::future<long>> futures;
        for (const std::string &hint : hints)
        {
            auto handle = std::async(std::launch::async, [login, password, hint]() -> std::vector<User>
                                     {
                                         try
                                        {
                                            Poco::Data::Session session = database::Database::get().create_session();
                                            Poco::Data::Statement select(session);
                                            long id;
                                            select  << "SELECT `user_id`" 
                                                    << "FROM User where `login`=? and `password`=?",
                                                into(id),
                                                use(login),
                                                use(password),
                                                range(0, 1); //  iterate over result set one row at a time

                                            select.execute();
                                            Poco::Data::RecordSet rs(select);
                                            if (rs.moveFirst()) return id;
                                        }
                                        catch (Poco::Data::MySQL::ConnectionException &e)
                                        {
                                            std::cout << e.displayText() << std::endl;
                                        }
                                        catch (Poco::Data::MySQL::StatementException &e)
                                        {
                                            std::cout << e.displayText() << std::endl;
                                        }

                                        return std::vector<User>(); });

            futures.emplace_back(std::move(handle));
        }
        
        for (std::future<long> &res : futures)
        {
            std::vector<long> v = res.get();
            std::copy(std::begin(v),
                      std::end(v),
                      std::back_inserter(result));
        }*/

        throw std::logic_error("not implemented yet");
        return 0;

    }

    std::optional<User> User::read_by_id(long id)
    {
        try
        {

            User rez;
            Poco::Data::Session session = database::Database::get().create_session();
            Poco::Data::Statement select(session);
            auto hint = Database::sharding_user(id);
            select  << "SELECT `user_id`, `first_name`, `last_name`, `email`, `phone`, `login`, `password`" 
                    << "FROM `user`" 
                    << "WHERE `user_id`=? " 
                    << hint,
                into(rez._id),
                into(rez._first_name),
                into(rez._last_name),
                into(rez._email),
                into(rez._phone),
                into(rez._login),
                into(rez._password),
                use(id),
                range(0, 1); //  iterate over result set one row at a time

            select.execute();

            Poco::Data::RecordSet rs(select);

            if (rs.moveFirst()) 
            {
                return rez;
            }

        }
        catch (Poco::Data::MySQL::ConnectionException &e)
        {
            std::cout << e.displayText() << std::endl;
        }
        catch (Poco::Data::MySQL::StatementException &e)
        {
            std::cout << e.displayText() << std::endl;
        }
        return {};
    }

    std::vector<User> User::read_all()
    {
        std::vector<User> result;

        std::vector<std::string> hints = database::Database::get_all_hints();
        std::vector<std::future<std::vector<User>>> futures;

        for (const std::string &hint : hints)
        {
            auto handle = std::async(std::launch::async, [hint]() -> std::vector<User>
                                     {
                                         try
                                        {
                                            Poco::Data::Session session = database::Database::get().create_session();
                                            Statement select(session);
                                            std::vector<User> result;
                                            User a;
                                            select  << "SELECT `user_id`, `first_name`, `last_name`, `email`, `phone`, `login`, `password`" 
                                                    << "FROM `user`"
                                                    << hint,
                                                into(a._id),
                                                into(a._first_name),
                                                into(a._last_name),
                                                into(a._email),
                                                into(a._phone),
                                                into(a._login),
                                                into(a._password),
                                                range(0, 1); //  iterate over result set one row at a time

                                            while (!select.done())
                                            {
                                                if (select.execute())
                                                    result.push_back(a);
                                            }
                                            return result;
                                        }
                                        catch (Poco::Data::MySQL::ConnectionException &e)
                                        {
                                            std::cout << e.displayText() << std::endl;
                                        }
                                        catch (Poco::Data::MySQL::StatementException &e)
                                        {
                                            std::cout << e.displayText() << std::endl;
                                        }
                                         
                                        return std::vector<User>(); });

            futures.emplace_back(std::move(handle));
        }
        
        for (std::future<std::vector<User>> &res : futures)
        {
            std::vector<User> v = res.get();
            std::copy(std::begin(v),
                      std::end(v),
                      std::back_inserter(result));
        }
        std::sort(std::begin(result), std::end(result), [](User a, User b)
                                                        {
                                                            return a.get_id() < b.get_id();
                                                        });

        return result;
    }

    std::vector<User> User::search(std::string first_name, std::string last_name)
    {
        std::vector<User> result;

        std::vector<std::string> hints = database::Database::get_all_hints();
        std::vector<std::future<std::vector<User>>> futures;

        for (const std::string &hint : hints)
        {
            auto handle = std::async(std::launch::async, [first_name, last_name, hint]() -> std::vector<User>
                                     {
                                        try
                                        {
                                            Poco::Data::Session session = database::Database::get().create_session();
                                            Statement select(session);
                                            std::vector<User> result;
                                            User a;
                                            auto fn = first_name + "%";
                                            auto ln = last_name + "%";
                                            select  << "SELECT `user_id`, `first_name`, `last_name`, `email`, `phone`, `login`, `password`" 
                                                    << "FROM `user`" 
                                                    << "WHERE `first_name` LIKE ? AND `last_name` LIKE ?"
                                                    << hint,
                                                into(a._id),
                                                into(a._first_name),
                                                into(a._last_name),
                                                into(a._email),
                                                into(a._phone),
                                                into(a._login),
                                                into(a._password),
                                                use(fn),
                                                use(ln),
                                                range(0, 1); //  iterate over result set one row at a time

                                            while (!select.done())
                                            {
                                                if (select.execute())
                                                    result.push_back(a);
                                            }
                                            return result;
                                        }
                                        catch (Poco::Data::MySQL::ConnectionException &e)
                                        {
                                            std::cout << e.displayText() << std::endl;
                                        }
                                        catch (Poco::Data::MySQL::StatementException &e)
                                        {
                                            std::cout << e.displayText() << std::endl;
                                        }

                                        return std::vector<User>(); });

            futures.emplace_back(std::move(handle));
        }
        
        for (std::future<std::vector<User>> &res : futures)
        {
            std::vector<User> v = res.get();
            std::copy(std::begin(v),
                      std::end(v),
                      std::back_inserter(result));
        }
        std::sort(std::begin(result), std::end(result), [](User a, User b)
                                                        {
                                                            return a.get_id() < b.get_id();
                                                        });

        return result;
    }

    bool User::save_to_mysql()
    {
        try
        {
            Poco::Data::Session session = database::Database::get().create_session();
            Poco::Data::Statement insert(session);
            _id = this->get_last_id();
            auto hint = Database::sharding_user(_id);
            insert  << "INSERT INTO `user` (`user_id`, `first_name`, `last_name`, `email`, `phone`, `login`, `password`)"
                    << "VALUES(?, ?, ?, ?, ?, ?, ?)"
                    << hint,
                use(_id),
                use(_first_name),
                use(_last_name),
                use(_email),
                use(_phone),
                use(_login),
                use(_password);

            insert.execute();

            std::cout << "inserted:" << _id << std::endl;
            return true;
        }
        catch (Poco::Data::MySQL::ConnectionException &e)
        {
            std::cout << e.displayText() << std::endl;
        }
        catch (Poco::Data::MySQL::StatementException &e)
        {
            std::cout << e.displayText() << std::endl;
        }
        return false;
    }

    void User::send_to_queue()
    {
        static cppkafka::Configuration config = {
            {"metadata.broker.list", Config::get().get_queue_host()},
            {"acks", "all"}};
        static cppkafka::Producer producer(config);
        static std::mutex mtx;
        static int message_key{0};
        using Hdr = cppkafka::MessageBuilder::HeaderType;

        std::lock_guard<std::mutex> lock(mtx);
        std::stringstream ss;
        Poco::JSON::Stringifier::stringify(toJSON(), ss);
        std::string message = ss.str();
        bool not_sent = true;

        cppkafka::MessageBuilder builder(Config::get().get_queue_topic());
        std::string mk = std::to_string(++message_key);
        builder.key(mk);                                       // set some key
        builder.header(Hdr{"producer_type", "user writer"}); // set some custom header
        builder.payload(message);                              // set message

        while (not_sent)
        {
            try
            {
                producer.produce(builder);
                not_sent = false;
            }
            catch (...)
            {
            }
        }
    }

    const long &User::get_last_id()
    {
        return ++(_last_id);
    }

    const std::string &User::get_login() const
    {
        return _login;
    }

    const std::string &User::get_password() const
    {
        return _password;
    }

    std::string &User::login()
    {
        return _login;
    }

    std::string &User::password()
    {
        return _password;
    }

    const long &User::get_id() const
    {
        return _id;
    }

    const std::string &User::get_first_name() const
    {
        return _first_name;
    }

    const std::string &User::get_last_name() const
    {
        return _last_name;
    }

    const std::string &User::get_email() const
    {
        return _email;
    }

    const std::string &User::get_phone() const
    {
        return _phone;
    }

    long &User::id()
    {
        return _id;
    }

    std::string &User::first_name()
    {
        return _first_name;
    }

    std::string &User::last_name()
    {
        return _last_name;
    }

    std::string &User::email()
    {
        return _email;
    }

    std::string &User::phone()
    {
        return _phone;
    }
}