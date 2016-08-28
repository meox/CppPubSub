//
// Created by meox on 27/08/16.
//

#ifndef PUBSUB_PUBSUB_HPP_H
#define PUBSUB_PUBSUB_HPP_H

#include <memory>
#include <vector>
#include <thread>
#include <queue>
#include <mutex>
#include <atomic>


namespace ps
{
    template <typename T>
    class Subscriber;

    template <typename T>
    class Topic;

    template <typename T>
    using topic_ptr_t = std::shared_ptr<Topic<T>>;

    template <typename T>
    using subscriber_ptr_t = std::shared_ptr<Subscriber<T>>;


    template <typename T>
    class Publisher
    {
    public:
        Publisher() = default;
        Publisher(const topic_ptr_t<T>& topic) : _topic{topic}
        {}

        void produce(const topic_ptr_t<T>& t, const T& msg) { t->send(msg); }

        void produce(const T& msg)
        {
            if(_topic)
                _topic->send(msg);
        }

    private:
        topic_ptr_t<T> _topic;
    };

    template <typename T>
    using publisher_ptr_t = std::shared_ptr<Publisher<T>>;


    template <typename T>
    class Topic
    {
    public:
        Topic() = default;
        Topic(std::string name) : _name{std::move(name)}
        {}

        void send(const T& data)
        {
            for(auto& sub : subs)
                sub->deliver(this, data);
        }

        void subscribe(subscriber_ptr_t<T> s)
        {
            subs.push_back(s);
        }

    private:
        std::string _name{};
        std::vector<std::shared_ptr<Subscriber<T>>> subs;
    };



    template <typename T>
    class Subscriber : public std::enable_shared_from_this<Subscriber<T>>
    {
    public:
        using f_callback_t = std::function<void(const Topic<T>* topic, T data)>;

        Subscriber() = default;

        Subscriber(f_callback_t f) : callaback{std::move(f)}
        {}


        void subscribe(const std::vector<topic_ptr_t<T>>& topics)
        {
            for (const auto& topic : topics)
                subscribe(topic);
        }

        void subscribe(const std::vector<topic_ptr_t<T>>& topics, f_callback_t f)
        {
            callaback = std::move(f);
            for (const auto& topic : topics)
                subscribe(topic);
        }

        void subscribe(const topic_ptr_t<T>& topic)
        {
            topic->subscribe(get_shared());
        }

        virtual void deliver(const Topic<T>* topic, const T& msg)
        {
            std::lock_guard<std::mutex> l(m);
            data.emplace(topic, msg);
        }

        void run()
        {
            stopped = false;
            th = std::thread([this](){
                while (true)
                {
                    std::unique_lock<std::mutex> g(m);
                    const auto is_empty = data.empty();
                    if (is_empty && stopped)
                        break;

                    if (!is_empty)
                    {
                        auto msg = data.front();
                        data.pop();
                        g.unlock();
                        execute(msg.first, msg.second);
                    }
                    else
                    {
                        g.unlock();
                        std::this_thread::sleep_for(std::chrono::milliseconds(3));
                    }
                }
            });
        }

        void unsubscribe()
        {
            if (!stopped)
            {
                stopped = true;
                th.join();
            }
        }

        virtual ~Subscriber()
        {
            unsubscribe();
        }

    protected:
        virtual void execute(const Topic<T>* topic, const T& data)
        {
            if(callaback)
                callaback(topic, data);
        }

        subscriber_ptr_t<T> get_shared()
        {
            return this->shared_from_this();
        }

    private:
        f_callback_t callaback;
        std::queue<std::pair<const Topic<T>*, T>> data;
        std::atomic<bool> stopped{true};
        std::mutex m;
        std::thread th;
    };


    template <typename T>
    topic_ptr_t<T> create_topic(const std::string& name)
    {
        return std::make_shared<Topic<T>>(name);
    }

    template <typename T>
    publisher_ptr_t<T> create_publisher(topic_ptr_t<T> topic)
    {
        return std::make_shared<Publisher<T>>(topic);
    }

    template <typename T>
    subscriber_ptr_t<T> create_subscriber(const topic_ptr_t<T>& topic, typename Subscriber<T>::f_callback_t&& f)
    {
        auto s = std::make_shared<Subscriber<T>>(f);
        s->subscribe(topic);
        return s;
    }

    template <typename T, typename F>
    subscriber_ptr_t<T> create_subscriber(const std::vector<topic_ptr_t<T>>& topics, F&& f)
    {
        auto s = std::make_shared<Subscriber<T>>(f);
        s->subscribe(topics);
        return s;
    }

    template <typename T, typename F>
    subscriber_ptr_t<T> create_subscriber(const std::initializer_list<topic_ptr_t<T>>& topics, F&& f)
    {
        std::vector<topic_ptr_t<T>> v_t{topics};
        return create_subscriber(v_t, std::forward<F>(f));
    }
}

#endif //PUBSUB_PUBSUB_HPP_H
