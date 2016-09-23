//
// Created by meox on 27/08/16.
//

#ifndef PUBSUB_PUBSUB_HPP_H
#define PUBSUB_PUBSUB_HPP_H

#include <memory>
#include <vector>
#include <thread>
#include <map>
#include <mutex>
#include <atomic>

#include <boost/lockfree/policies.hpp>
#include <boost/lockfree/queue.hpp>
#include <set>
#include <queue>


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
		Publisher(const topic_ptr_t<T>& topic) : _topic{topic}
		{
			_topic->attach(this);
		}

		void produce(T&& msg)
		{
			_topic->send(std::forward<T>(msg));
		}

		virtual void signal(int /*type_signal*/) {
		}

		size_t get_id() const {	return id; }
		virtual ~Publisher()
		{
			_topic->detach(this);
		}

	private:
		static size_t get_counter()
		{
			static std::size_t counter{};
			counter++;
			return counter;
		}

	private:
		const size_t id{get_counter()};
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

		void send(T&& data)
		{
			for(auto& sub : subs)
				sub.second->deliver(this, std::forward<T>(data));
		}

		void attach(Publisher<T>* s)
		{
			std::lock_guard<std::mutex> l{m};
			pubs[s->get_id()] = s;
		}

		void detach(Publisher<T>* s)
		{
			std::lock_guard<std::mutex> l{m};
			pubs.erase(s->get_id());
		}

		void subscribe(Subscriber<T>* s)
		{
			std::lock_guard<std::mutex> l{m};
			const auto id = s->get_id();
			subs[id] = s;
			signals[id].clear();
		}

		void unsubscribe(Subscriber<T>* s)
		{
			std::lock_guard<std::mutex> l{m};
			subs.erase(s->get_id());
			signals.erase(s->get_id());
		}

		void signal(int type_signal, size_t subscriber_id)
		{
			std::lock_guard<std::mutex> l{m};
			signals[subscriber_id].insert(type_signal);

			const auto num_signals = std::count_if(signals.begin(), signals.end(), [type_signal](const auto& e){
				return e.second.find(type_signal) != e.second.end();
			});

			if (num_signals > 0 && static_cast<size_t>(num_signals) == subs.size())
			{
				for (auto& e : signals)
					e.second.erase(type_signal);

				for (auto& p : pubs)
					p.second->signal(type_signal);
			}
		}

		size_t get_id() const { return id; }

		virtual ~Topic(){}

	private:
		static size_t get_counter()
		{
			static std::size_t counter{};
			return counter++;
		}

		std::mutex m;
		size_t id{get_counter()};
		std::string _name{};
		std::map<size_t, Subscriber<T>*> subs;
		std::map<size_t, Publisher<T>*> pubs;
		std::map<size_t, std::set<int>> signals{};
	};


#pragma pack(push, 1)
	template <typename T>
	struct msg_container_t
	{
		const Topic<T>* topic_ptr{nullptr};
		T data;
	};
#pragma pack(pop)


	template <typename T>
	class Subscriber
	{
	public:
		using queue_t = boost::lockfree::queue<msg_container_t<T>, boost::lockfree::fixed_sized<true>, boost::lockfree::capacity<55535ul>>;
		using topic_raw_ptr = const Topic<T>*;
		using data_t = const T&;
		using f_callback_t = std::function<void(topic_raw_ptr topic, data_t data)>;

		Subscriber() = default;

		void subscribe(const std::vector<topic_ptr_t<T>>& topics)
		{
			for (const auto& topic : topics)
				subscribe(topic);
		}

		void subscribe(const std::initializer_list<topic_ptr_t<T>>& topics)
		{
			for (const auto& topic : topics)
				subscribe(topic);
		}

		void subscribe(const topic_ptr_t<T>& topic)
		{
			std::lock_guard<std::mutex> l{m};
			topic->subscribe(this);
			topics[topic->get_id()] = topic;
		}

		size_t num_topics() const { return topics.size(); }

		void unsubscribe(const topic_ptr_t<T>& topic)
		{
			std::lock_guard<std::mutex> l{m};
			topic->unsubscribe(this);
			topics.erase(topic->get_id());
		}

		void set_callback(f_callback_t f)
		{
			extecute_callback = std::move(f);
		}

		virtual void deliver(topic_raw_ptr topic, T e)
		{
			msg_container_t<T> msg;
			msg.topic_ptr = topic;
			msg.data = e;

			while (true)
			{
				bool b = data.push(msg);
				if (b)
					break;
				else
					std::this_thread::sleep_for(std::chrono::milliseconds(50));
			}
		}

		void run()
		{
			th = std::thread([this](){
				{
					std::lock_guard<std::mutex> l(m);
					stopped = false;
				}
				event_loop();
				{
					std::lock_guard<std::mutex> l(m);
					stopped = true;
				}
			});
		}

		void wait()
		{
			std::unique_lock<std::mutex> l(m);
			if (!stopped)
			{
				l.unlock();
				th.join();
			}
		}

		void stop()
		{
			std::lock_guard<std::mutex> l(m);
			if (!stopped)
				to_stop = true;
		}

		virtual ~Subscriber()
		{
			std::unique_lock<std::mutex> l(m);
			if (!stopped)
				to_stop = true;
			l.unlock();

			wait();
		}

		size_t get_id() const { return id; }

	protected:
		static size_t get_counter()
		{
			static std::size_t counter{};
			return counter++;
		}

		virtual void event_loop()
		{
			while (true)
			{
				msg_container_t<T> msg{};
				bool data_in_queue{false};
				const auto is_data = data.pop(msg);
				if (is_data)
				{
					data_in_queue = true;
					execute(msg.topic_ptr, msg.data);
				}

				if (!data_in_queue)
				{
					{
						std::lock_guard<std::mutex> l(m);
						if (to_stop)
							break;
					}

					std::this_thread::sleep_for(std::chrono::milliseconds(50));
				}
			}
		}

		void emit_signal(int type_signal)
		{
			std::lock_guard<std::mutex> l(m);
			const auto id = get_id();
			for (const auto& t : topics)
				t.second->signal(type_signal, id);
		}

		virtual void execute(topic_raw_ptr topic, data_t data)
		{
			extecute_callback(topic, data);
		}

	private:
		std::mutex m;
		queue_t data;
		f_callback_t extecute_callback;
		bool stopped{true};
		bool to_stop{false};
		std::map<size_t, topic_ptr_t<T>> topics;
		std::thread th;
		const size_t id{get_counter()};
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

	template <typename T, typename Q>
	std::shared_ptr<Q> create_publisher(topic_ptr_t<T> topic)
	{
		return std::make_shared<Q>(topic);
	}

	template <typename T, typename F>
	subscriber_ptr_t<T> create_subscriber(const topic_ptr_t<T>& topic, F&& f)
	{
		auto s = std::make_shared<Subscriber<T>>();
		s->subscribe(topic);
		s->set_callback(std::forward<F>(f));
		return s;
	}

	template <typename T, typename F>
	subscriber_ptr_t<T> create_subscriber(const std::vector<topic_ptr_t<T>>& topics, F&& f)
	{
		auto s = std::make_shared<Subscriber<T>>();
		s->subscribe(topics);
		s->set_callback(std::forward<F>(f));
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
