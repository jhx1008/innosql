/*
 * Copyright (c) 2015, 2016 Oracle and/or its affiliates. All rights reserved.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License as
 * published by the Free Software Foundation; version 2 of the
 * License.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301  USA
 */

#include <google/protobuf/text_format.h>
#include "mysqlx.h"
#include "mysqlx_crud.h"
#include "mysqlx_connection.h"
#include "ngs_common/protocol_const.h"

#include <google/protobuf/dynamic_message.h>
#include <google/protobuf/io/zero_copy_stream.h>
#include <google/protobuf/io/tokenizer.h>

#include <boost/algorithm/string.hpp>
#include <boost/algorithm/hex.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/bind.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/date_time/posix_time/posix_time_types.hpp>
#include <string.h>
#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <boost/format.hpp>
#include <boost/make_shared.hpp>

#include "expr_parser.h"
#include "utils_mysql_parsing.h"

#include "violite.h"
#include "m_string.h" // needed by writer.h, but has to be included after expr_parser.h
#include <rapidjson/writer.h>

#include <ios>
#include <iostream>
#include <fstream>
#include <iterator>
#include <sstream>
#include <stdexcept>
#include <algorithm>

const char CMD_ARG_SEPARATOR = '\t';

#include <mysql/service_my_snprintf.h>

#ifdef _MSC_VER
#  pragma push_macro("ERROR")
#  undef ERROR
#endif

using namespace google::protobuf;

typedef std::map<std::string, std::string> Message_by_full_name;
static Message_by_full_name server_msgs_by_full_name;
static Message_by_full_name client_msgs_by_full_name;

typedef std::map<std::string, std::pair<mysqlx::Message* (*)(), int8_t> > Message_by_name;
typedef boost::function<void (std::string)> Value_callback;
static Message_by_name server_msgs_by_name;
static Message_by_name client_msgs_by_name;

typedef std::map<int8_t, std::pair<mysqlx::Message* (*)(), std::string> > Message_by_id;
static Message_by_id server_msgs_by_id;
static Message_by_id client_msgs_by_id;

bool OPT_quiet = false;
bool OPT_bindump = false;
bool OPT_show_warnings = false;
bool OPT_fatal_errors = false;
bool OPT_verbose = false;
#ifndef _WIN32
bool OPT_color = false;
#endif

class Expected_error;
static Expected_error *OPT_expect_error = 0;

struct Stack_frame {
  int line_number;
  std::string context;
};
static std::list<Stack_frame> script_stack;

static std::map<std::string, std::string> variables;
static std::list<std::string> variables_to_unreplace;

void replace_all(std::string &input, const std::string &to_find, const std::string &change_to)
{
  size_t position = input.find(to_find);

  while (std::string::npos != position)
  {
    input.replace(position, to_find.size(), change_to);
    position = input.find(to_find, position + change_to.size());
  }
}

void replace_variables(std::string &s)
{
  for (std::map<std::string, std::string>::const_iterator sub = variables.begin();
      sub != variables.end(); ++sub)
  {
    std::string tmp(sub->second);

    // change from boost::replace_all to own fast forward implementation
    // which is 2 times faster than boost (tested in debug mode)
    replace_all(tmp, "\"", "\\\"");
    replace_all(tmp, "\n", "\\n");
    replace_all(s, sub->first, tmp);
  }
}

std::string unreplace_variables(const std::string &in, bool clear)
{
  std::string s = in;
  for (std::list<std::string>::const_iterator sub = variables_to_unreplace.begin();
      sub != variables_to_unreplace.end(); ++sub)
  {
    replace_all(s, variables[*sub], *sub);
  }
  if (clear)
    variables_to_unreplace.clear();
  return s;
}

static std::string error()
{
  std::string context;

  for (std::list<Stack_frame>::const_reverse_iterator it = script_stack.rbegin(); it != script_stack.rend(); ++it)
  {
    char tmp[1024];
    my_snprintf(tmp, sizeof(tmp), "in %s, line %i:", it->context.c_str(), it->line_number);
    context.append(tmp);
  }

#ifndef _WIN32
  if (OPT_color)
    return std::string("\e[1;31m").append(context).append("ERROR: ");
  else
#endif
    return std::string(context).append("ERROR: ");
}

static std::string eoerr()
{
#ifndef _WIN32
  if (OPT_color)
    return "\e[0m\n";
  else
#endif
    return "\n";
}

static void dumpx(const std::exception &exc)
{
  std::cerr << error() << exc.what() << eoerr();
}

static void dumpx(const mysqlx::Error &exc)
{
  std::cerr << error() << exc.what() << " (code " << exc.error() << ")" << eoerr();
}

static void print_columndata(const std::vector<mysqlx::ColumnMetadata> &meta);
static void print_result_set(mysqlx::Result &result);
static void print_result_set(mysqlx::Result &result, const std::vector<std::string> &columns, Value_callback value_callback = Value_callback());

static std::string message_to_text(const mysqlx::Message &message);

//---------------------------------------------------------------------------------------------------------

class Expected_error
{
public:
  Expected_error() {}

  void expect_errno(int err)
  {
    m_expect_errno.insert(err);
  }

  bool check_error(const mysqlx::Error &err)
  {
    if (m_expect_errno.empty())
    {
      dumpx(err);
      return !OPT_fatal_errors;
    }

    return check(err);
  }

  bool check_ok()
  {
    if (m_expect_errno.empty())
      return true;
    return check(mysqlx::Error());
  }

private:
  bool check(const mysqlx::Error &err)
  {
    if (m_expect_errno.find(err.error()) == m_expect_errno.end())
    {
      print_unexpected_error(err);
      m_expect_errno.clear();
      return !OPT_fatal_errors;
    }

    print_expected_error(err);
    m_expect_errno.clear();
    return true;
  }

  void print_unexpected_error(const mysqlx::Error &err)
  {
    std::cerr << error() << "Got unexpected error";
    print_error_msg(std::cerr, err);
    std::cerr << "; expected was ";
    if (m_expect_errno.size() > 1)
      std::cerr << "one of: ";
    print_expect_errors(std::cerr);
    std::cerr << "\n";
  }

  void print_expected_error(const mysqlx::Error &err)
  {
    std::cout << "Got expected error";
    if (m_expect_errno.size() == 1)
      print_error_msg(std::cout, err);
    else
    {
      std::cout << " (one of: ";
      print_expect_errors(std::cout);
      std::cout << ")";
    }
    std::cout << "\n";
  }

  void print_error_msg(std::ostream & os, const mysqlx::Error &err)
  {
    if (err.error())
      os << ": " << err.what();
    os << " (code " << err.error() << ")";
  }

  void print_expect_errors(std::ostream & os)
  {
    std::copy(m_expect_errno.begin(),
              m_expect_errno.end(),
              std::ostream_iterator<int>(os, " "));
  }

  std::set<int> m_expect_errno;
};

//---------------------------------------------------------------------------------------------------------

class Connection_manager
{
public:
  Connection_manager(const std::string &uri, const mysqlx::Ssl_config &ssl_config_, const std::size_t timeout_, const bool _dont_wait_for_disconnect)
  : ssl_config(ssl_config_), timeout(timeout_), dont_wait_for_disconnect(_dont_wait_for_disconnect)
  {
    int pwdfound;
    mysqlx::parse_mysql_connstring(uri, proto, user, pass, host, port, sock, db, pwdfound);

    active_connection.reset(new mysqlx::Connection(ssl_config, timeout, dont_wait_for_disconnect));
    connections[""] = active_connection;

    if (OPT_verbose)
      std::cout << "Connecting...\n";
    active_connection->connect(host, port);
  }

  void get_credentials(std::string &ret_user, std::string &ret_pass)
  {
    ret_user = user;
    ret_pass = pass;
  }

  void connect_default(const bool send_cap_password_expired = false, bool use_plain_auth = false)
  {
    if (send_cap_password_expired)
      active_connection->setup_capability("client.pwd_expire_ok", true);

    if (use_plain_auth)
      active_connection->authenticate_plain(user, pass, db);
    else
      active_connection->authenticate(user, pass, db);

    std::stringstream s;
    s << active_connection->client_id();
    variables["%ACTIVE_CLIENT_ID%"] = s.str();

    if (OPT_verbose)
      std::cout << "Connected client #" << active_connection->client_id() << "\n";
  }

  void create(const std::string &name,
              const std::string &user, const std::string &password, const std::string &db,
              bool no_ssl)
  {
    if (connections.find(name) != connections.end())
      throw std::runtime_error("a session named "+name+" already exists");

    std::cout << "connecting...\n";

    boost::shared_ptr<mysqlx::Connection> connection;
    mysqlx::Ssl_config                    connection_ssl_config;

    if (!no_ssl)
      connection_ssl_config = ssl_config;

    connection.reset(new mysqlx::Connection(connection_ssl_config, timeout, dont_wait_for_disconnect));

    connection->connect(host, port);
    if (user != "-")
    {
      if (user.empty())
        connection->authenticate(this->user, this->pass, db.empty() ? this->db : db);
      else
        connection->authenticate(user, password, db.empty() ? this->db : db);
    }

    active_connection = connection;
    active_connection_name = name;
    connections[name] = active_connection;
    std::stringstream s;
    s << active_connection->client_id();
    variables["%ACTIVE_CLIENT_ID%"] = s.str();
    std::cout << "active session is now '" << name << "'\n";

    if (OPT_verbose)
      std::cout << "Connected client #" << active_connection->client_id() << "\n";
  }

  void abort_active()
  {
    if (active_connection)
    {
      if (!active_connection_name.empty())
        std::cout << "aborting session " << active_connection_name << "\n";
      active_connection->set_closed();
      active_connection.reset();
      connections.erase(active_connection_name);
      if (active_connection_name != "")
        set_active("");
    }
    else
      throw std::runtime_error("no active session");
  }

  bool is_default_active()
  {
    return active_connection_name.empty();
  }

  void close_active(bool shutdown = false)
  {
    if (active_connection)
    {
      if (active_connection_name.empty() && !shutdown)
        throw std::runtime_error("cannot close default session");
      try
      {
        if (!active_connection_name.empty())
          std::cout << "closing session " << active_connection_name << "\n";

        if (!active_connection->is_closed())
        {
          // send a close message and wait for the corresponding Ok message
          active_connection->send(Mysqlx::Session::Close());
          active_connection->set_closed();
          int msgid;
          boost::scoped_ptr<mysqlx::Message> msg(active_connection->recv_raw(msgid));
          std::cout << message_to_text(*msg);
          if (Mysqlx::ServerMessages::OK != msgid)
            throw mysqlx::Error(CR_COMMANDS_OUT_OF_SYNC,
                                "Disconnect was expecting Mysqlx.Ok(bye!), but got the one above (one or more calls to -->recv are probably missing)");

          std::string text = static_cast<Mysqlx::Ok*>(msg.get())->msg();
          if (text != "bye!" && text != "tchau!")
            throw mysqlx::Error(CR_COMMANDS_OUT_OF_SYNC,
                                "Disconnect was expecting Mysqlx.Ok(bye!), but got the one above (one or more calls to -->recv are probably missing)");

          if (!dont_wait_for_disconnect)
          {
            try
            {
              boost::scoped_ptr<mysqlx::Message> msg(active_connection->recv_raw(msgid));

              std::cout << message_to_text(*msg);

              throw mysqlx::Error(CR_COMMANDS_OUT_OF_SYNC,
                  "Was expecting closure but got the one above message");
            }
            catch (...)
            {}
          }
        }
        connections.erase(active_connection_name);
        if (!shutdown)
          set_active("");
      }
      catch (...)
      {
        connections.erase(active_connection_name);
        if (!shutdown)
          set_active("");
        throw;
      }
    }
    else if (!shutdown)
      throw std::runtime_error("no active session");
  }

  void set_active(const std::string &name)
  {
    if (connections.find(name) == connections.end())
    {
      std::string slist;
      for (std::map<std::string, boost::shared_ptr<mysqlx::Connection> >::const_iterator it = connections.begin(); it != connections.end(); ++it)
        slist.append(it->first).append(", ");
      if (!slist.empty())
        slist.resize(slist.length()-2);
      throw std::runtime_error("no session named '"+name+"': " + slist);
    }
    active_connection = connections[name];
    active_connection_name = name;
    std::stringstream s;
    s << active_connection->client_id();
    variables["%ACTIVE_CLIENT_ID%"] = s.str();
    std::cout << "switched to session " << (active_connection_name.empty() ? "default" : active_connection_name) << "\n";
  }

  mysqlx::Connection* active()
  {
    if (!active_connection)
      std::runtime_error("no active session");
    return active_connection.get();
  }

private:
  std::map<std::string, boost::shared_ptr<mysqlx::Connection> > connections;
  boost::shared_ptr<mysqlx::Connection> active_connection;
  std::string active_connection_name;
  std::string proto, user, pass, host, sock, db;
  int port;
  mysqlx::Ssl_config ssl_config;
  const std::size_t timeout;
  const bool dont_wait_for_disconnect;
};

static std::string data_to_bindump(const std::string &bindump)
{
  std::string res;

  for (size_t i = 0; i < bindump.length(); i++)
  {
    unsigned char ch = bindump[i];

    if (i >= 5 && ch == '\\')
    {
      res.push_back('\\');
      res.push_back('\\');
    }
    else if (i >= 5 && isprint(ch) && !isblank(ch))
      res.push_back(ch);
    else
    {
      static const char *hex = "0123456789abcdef";
      res.append("\\x");
      res.push_back(hex[(ch >> 4) & 0xf]);
      res.push_back(hex[ch & 0xf]);
    }
  }

  return res;
}

static std::string bindump_to_data(const std::string &bindump)
{
  std::string res;
  for (size_t i = 0; i < bindump.length(); i++)
  {
    if (bindump[i] == '\\')
    {
      if (bindump[i+1] == '\\')
      {
        res.push_back('\\');
        ++i;
      }
      else if (bindump[i+1] == 'x')
      {
        int value = 0;
        static const char *hex = "0123456789abcdef";
        const char *p = strchr(hex, bindump[i+2]);
        if (p)
          value = (p - hex) << 4;
        else
        {
          std::cerr << error() << "Invalid bindump char at " << i+2 << eoerr();
          break;
        }
        p = strchr(hex, bindump[i+3]);
        if (p)
          value |= p - hex;
        else
        {
          std::cerr << error() << "Invalid bindump char at " << i+3 << eoerr();
          break;
        }
        i += 3;
        res.push_back(value);
      }
    }
    else
      res.push_back(bindump[i]);
  }
  return res;
}

static std::string message_to_text(const mysqlx::Message &message)
{
  std::string output;
  std::string name;

  google::protobuf::TextFormat::Printer printer;

  // special handling for nested messages (at least for Notices)
  if (message.GetDescriptor()->full_name() == "Mysqlx.Notice.Frame")
  {
    Mysqlx::Notice::Frame frame = *static_cast<const Mysqlx::Notice::Frame*>(&message);
    switch (frame.type())
    {
    case 1: // warning
    {
      Mysqlx::Notice::Warning subm;
      subm.ParseFromString(frame.payload());
      printer.PrintToString(subm, &output);
      frame.set_payload(subm.GetDescriptor()->full_name() + " { " + output + " }");
      break;
    }
    case 2: // session variable
    {
      Mysqlx::Notice::SessionVariableChanged subm;
      subm.ParseFromString(frame.payload());
      printer.PrintToString(subm, &output);
      frame.set_payload(subm.GetDescriptor()->full_name() + " { " + output + " }");
      break;
    }
    case 3: // session state
    {
      Mysqlx::Notice::SessionStateChanged subm;
      subm.ParseFromString(frame.payload());
      printer.PrintToString(subm, &output);
      frame.set_payload(subm.GetDescriptor()->full_name() + " { " + output + " }");
      break;
    }
    }
    printer.SetInitialIndentLevel(1);
    printer.PrintToString(frame, &output);
  }
  else
  {
    printer.SetInitialIndentLevel(1);
    printer.PrintToString(message, &output);
  }

  return message.GetDescriptor()->full_name() + " {\n" + output + "}\n";
}

static std::string message_to_bindump(const mysqlx::Message &message)
{
  std::string res;
  std::string out;

  message.SerializeToString(&out);

  res.resize(5);
  *(uint32_t*)res.data() = out.size() + 1;

#ifdef WORDS_BIGENDIAN
  std::swap(res[0], res[3]);
  std::swap(res[1], res[2]);
#endif

  res[4] = client_msgs_by_name[client_msgs_by_full_name[message.GetDescriptor()->full_name()]].second;
  res.append(out);

  return data_to_bindump(res);
}

/*
static mysqlx::Message *text_to_server_message(const std::string &name, const std::string &data)
{
  if (server_msgs_by_full_name.find(name) == server_msgs_by_full_name.end())
  {
    std::cerr << "Invalid message type " << name << "\n";
    return NULL;
  }
  mysqlx::Message *message = server_msgs_by_name[server_msgs_by_full_name[name]].first();

  google::protobuf::TextFormat::ParseFromString(data, message);

  return message;
}
*/

class ErrorDumper : public ::google::protobuf::io::ErrorCollector
{
  std::stringstream m_out;

public:
  virtual void AddError(int line, int column, const string & message)
  {
    m_out << "ERROR in message: line " << line+1 << ": column " << column << ": " << message<<"\n";
  }

  virtual void AddWarning(int line, int column, const string & message)
  {
    m_out << "WARNING in message: line " << line+1 << ": column " << column << ": " << message<<"\n";
  }

  std::string str() { return m_out.str(); }
};

static mysqlx::Message *text_to_client_message(const std::string &name, const std::string &data, int8_t &msg_id)
{
  if (client_msgs_by_full_name.find(name) == client_msgs_by_full_name.end())
  {
    std::cerr << error() << "Invalid message type " << name << eoerr();
    return NULL;
  }

  Message_by_name::const_iterator msg = client_msgs_by_name.find(client_msgs_by_full_name[name]);
  if (msg == client_msgs_by_name.end())
  {
    std::cerr << error() << "Invalid message type " << name << eoerr();
    return NULL;
  }

  mysqlx::Message *message = msg->second.first();
  msg_id = msg->second.second;

  google::protobuf::TextFormat::Parser parser;
  ErrorDumper dumper;
  parser.RecordErrorsTo(&dumper);
  if (!parser.ParseFromString(data, message))
  {
    std::cerr << error() << "Invalid message in input: " << name << eoerr();
    int i = 1;
    for (std::string::size_type p = 0, n = data.find('\n', p+1);
        p != std::string::npos;
        p = (n == std::string::npos ? n : n+1), n = data.find('\n', p+1), ++i)
    {
      std::cerr << i << ": " << data.substr(p, n-p) << "\n";
    }
    std::cerr << "\n" << dumper.str();
    delete message;
    return NULL;
  }

  return message;
}

static bool dump_notices(int type, const std::string &data)
{
  if (type == 3)
  {
    Mysqlx::Notice::SessionStateChanged change;
    change.ParseFromString(data);
    if (!change.IsInitialized())
      std::cerr << "Invalid notice received from server " << change.InitializationErrorString() << "\n";
    else
    {
      if (change.param() == Mysqlx::Notice::SessionStateChanged::ACCOUNT_EXPIRED)
      {
        std::cout << "NOTICE: Account password expired\n";
        return true;
      }
    }
  }
  return false;
}

//-----------------------------------------------------------------------------------

class Execution_context
{
public:
  Execution_context(std::istream &stream, Connection_manager *cm)
  : m_stream(stream), m_cm(cm)
  { }

  std::string         m_command_name;
  std::istream       &m_stream;
  Connection_manager *m_cm;

  mysqlx::Connection *connection() { return m_cm->active(); }
};

//---------------------------------------------------------------------------------------------------------

class Macro
{
public:
  Macro(const std::string &name, const std::list<std::string> &argnames)
  : m_name(name), m_args(argnames)
  { }

  std::string name() const { return m_name; }

  void set_body(const std::string &body)
  {
    m_body = body;
  }

  std::string get(const std::list<std::string> &args) const
  {
    if (args.size() != m_args.size())
    {
      std::cerr << error() << "Invalid number of arguments for macro "+m_name << ", expected:" << m_args.size() << " actual:" << args.size() << eoerr();
      return "";
    }

    std::string text = m_body;
    std::list<std::string>::const_iterator n = m_args.begin(), v = args.begin();
    for (size_t i = 0; i < args.size(); i++)
    {
      replace_all(text, *(n++), *(v++));
    }
    return text;
  }

public:
  static std::list<boost::shared_ptr<Macro> > macros;

  static void add(boost::shared_ptr<Macro> macro)
  {
    macros.push_back(macro);
  }

  static std::string get(const std::string &cmd, std::string &r_name)
  {
    std::list<std::string> args;
    std::string::size_type p = std::min(cmd.find(' '), cmd.find('\t'));
    if (p == std::string::npos)
      r_name = cmd;
    else
    {
      r_name = cmd.substr(0, p);
      std::string rest = cmd.substr(p+1);
      boost::split(args, rest, boost::is_any_of("\t"), boost::token_compress_on);
    }
    if (r_name.empty())
    {
      std::cerr << error() << "Missing macro name for macro call" << eoerr();
      return "";
    }

    for (std::list<boost::shared_ptr<Macro> >::const_iterator iter = macros.begin(); iter != macros.end(); ++iter)
    {
      if ((*iter)->m_name == r_name)
      {
        return (*iter)->get(args);
      }
    }
    std::cerr << error() << "Undefined macro " << r_name << eoerr();
    return "";
  }

  static bool call(Execution_context &context, const std::string &cmd);

private:
  std::string m_name;
  std::list<std::string> m_args;
  std::string m_body;
};

std::list<boost::shared_ptr<Macro> > Macro::macros;

//---------------------------------------------------------------------------------------------------------

class Command
{
public:
  enum Result {Continue, Stop_with_success, Stop_with_failure};

  Command()
  : m_cmd_prefix("-->")
  {
    m_commands["title "]      = &Command::cmd_title;
    m_commands["echo "]       = &Command::cmd_echo;
    m_commands["recvtype "]   = &Command::cmd_recvtype;
    m_commands["recverror "]  = &Command::cmd_recverror;
    m_commands["recvresult"]  = &Command::cmd_recvresult;
    m_commands["recvresult "] = &Command::cmd_recvresult;
    m_commands["recvtovar "]  = &Command::cmd_recvtovar;
    m_commands["recvuntil "]  = &Command::cmd_recvuntil;
    m_commands["recvuntildisc"] = &Command::cmd_recv_all_until_disc;
    m_commands["enablessl"]   = &Command::cmd_enablessl;
    m_commands["sleep "]      = &Command::cmd_sleep;
    m_commands["login "]      = &Command::cmd_login;
    m_commands["stmtadmin "]  = &Command::cmd_stmt;
    m_commands["stmtsql "]    = &Command::cmd_stmt;
    m_commands["loginerror "] = &Command::cmd_loginerror;
    m_commands["repeat "]     = &Command::cmd_repeat;
    m_commands["endrepeat"]   = &Command::cmd_endrepeat;
    m_commands["system "]     = &Command::cmd_system;
    m_commands["peerdisc "]   = &Command::cmd_peerdisc;
    m_commands["recv"]        = &Command::cmd_recv;
    m_commands["exit"]        = &Command::cmd_exit;
    m_commands["abort"]        = &Command::cmd_abort;
    m_commands["nowarnings"]  = &Command::cmd_nowarnings;
    m_commands["yeswarnings"] = &Command::cmd_yeswarnings;
    m_commands["fatalerrors"] = &Command::cmd_fatalerrors;
    m_commands["nofatalerrors"] = &Command::cmd_nofatalerrors;
    m_commands["newsession "]  = &Command::cmd_newsession;
    m_commands["newsessionplain "]  = &Command::cmd_newsessionplain;
    m_commands["setsession "]  = &Command::cmd_setsession;
    m_commands["setsession"]  = &Command::cmd_setsession; // for setsession with no args
    m_commands["closesession"]= &Command::cmd_closesession;
    m_commands["expecterror "] = &Command::cmd_expecterror;
    m_commands["measure"]      = &Command::cmd_measure;
    m_commands["endmeasure "]  = &Command::cmd_endmeasure;
    m_commands["quiet"]        = &Command::cmd_quiet;
    m_commands["noquiet"]      = &Command::cmd_noquiet;
    m_commands["varfile "]     = &Command::cmd_varfile;
    m_commands["varlet "]      = &Command::cmd_varlet;
    m_commands["varinc "]      = &Command::cmd_varinc;
    m_commands["varsub "]      = &Command::cmd_varsub;
    m_commands["vargen "]      = &Command::cmd_vargen;
    m_commands["binsend "]     = &Command::cmd_binsend;
    m_commands["hexsend "]     = &Command::cmd_hexsend;
    m_commands["binsendoffset "] = &Command::cmd_binsendoffset;
    m_commands["callmacro "]   = &Command::cmd_callmacro;
    m_commands["import "]      = &Command::cmd_import;
  }

  bool is_command_syntax(const std::string &cmd) const
  {
    return 0 == strncmp(cmd.c_str(), m_cmd_prefix.c_str(), m_cmd_prefix.length());
  }

  Result process(Execution_context &context, const std::string &command)
  {
    if (!is_command_syntax(command))
      return Stop_with_failure;

    Command_map::iterator i = std::find_if(m_commands.begin(),
                                           m_commands.end(),
                                           boost::bind(&Command::match_command_name, this, _1, command));

    if (i == m_commands.end())
    {
      std::cerr << "Unknown command " << command << "\n";
      return Stop_with_failure;
    }

    if (OPT_verbose)
      std::cout << "Execute " << command <<"\n";

    context.m_command_name = (*i).first;

    return (*this.*(*i).second)(context, command.c_str() + m_cmd_prefix.length() + (*i).first.length());
  }

private:
  typedef std::map< std::string, Result (Command::*)(Execution_context &,const std::string &) > Command_map;

  struct Loop_do
  {
    std::streampos block_begin;
    int            iterations;
    int            value;
    std::string    variable_name;
  };

  Command_map        m_commands;
  std::list<Loop_do> m_loop_stack;
  std::string        m_cmd_prefix;

  bool match_command_name(const Command_map::value_type &command, const std::string &instruction)
  {
    if (m_cmd_prefix.length() + command.first.length() > instruction.length())
      return false;

    std::string::const_iterator i = std::find(instruction.begin(), instruction.end(), ' ');
    std::string                 command_name(instruction.begin() + m_cmd_prefix.length(), i);

    if (0 != command.first.compare(command_name))
    {
      if (instruction.end() != i)
      {
        ++i;
        return 0 == command.first.compare(std::string(instruction.begin() + m_cmd_prefix.length(), i));
      }

      return false;
    }

    return true;
  }

  Result cmd_echo(Execution_context &context, const std::string &args)
  {
    std::string s = args;
    replace_variables(s);
    std::cout << s << "\n";

    return Continue;
  }

  Result cmd_title(Execution_context &context, const std::string &args)
  {
    if (!args.empty())
    {
      std::cout << "\n" << args.substr(1) << "\n";
      std::string sep(args.length()-1, args[0]);
      std::cout << sep << "\n";
    }
    else
      std::cout << "\n\n";

    return Continue;
  }

  Result cmd_recvtype(Execution_context &context, const std::string &args)
  {
    int msgid;
    boost::scoped_ptr<mysqlx::Message> msg(context.connection()->recv_raw(msgid));
    if (msg.get())
    {
      if (msg->GetDescriptor()->full_name() != args)
        std::cout << "Received unexpected message. Was expecting:\n    " << args << "\nbut got:\n";
      try
      {
        std::cout << message_to_text(*msg) << "\n";

        if (msg->GetDescriptor()->full_name() != args && OPT_fatal_errors)
          return Stop_with_success;
      }
      catch (std::exception &e)
      {
        dumpx(e);
        if (OPT_fatal_errors)
          return Stop_with_success;
      }
    }

    return Continue;
  }

  Result cmd_recverror(Execution_context &context, const std::string &args)
  {
    int msgid;
    boost::scoped_ptr<mysqlx::Message> msg(context.connection()->recv_raw(msgid));
    if (msg.get())
    {
      bool failed = false;
      if (msg->GetDescriptor()->full_name() != "Mysqlx.Error" || (uint32_t)atoi(args.c_str()) != static_cast<Mysqlx::Error*>(msg.get())->code())
      {
        std::cout << error() << "Was expecting Error " << args <<", but got:" << eoerr();
        failed = true;
      }
      else
      {
        std::cout << "Got expected error:\n";
      }
      try
      {
        std::cout << message_to_text(*msg) << "\n";
        if (failed && OPT_fatal_errors)
          return Stop_with_success;
      }
      catch (std::exception &e)
      {
        dumpx(e);
        if (OPT_fatal_errors)
          return Stop_with_success;
      }
    }

    return Continue;
  }

  static void set_variable(bool &was_set, std::string name, std::string value)
  {
    was_set = true;

    variables[name] = value;
  }

  Result cmd_recvtovar(Execution_context &context, const std::string &args)
  {
    bool was_set = false;

    cmd_recvresult(context, "", boost::bind(&Command::set_variable, boost::ref(was_set), args, _1));

    if (!was_set)
    {
      std::cerr << "No data received from query\n";
      return Stop_with_failure;
    }

    return Continue;
  }

  Result cmd_recvresult(Execution_context &context, const std::string &args)
  {
    return cmd_recvresult(context, args, Value_callback());
  }

  Result cmd_recvresult(Execution_context &context, const std::string &args, Value_callback value_callback)
  {
    boost::shared_ptr<mysqlx::Result> result;
    try
    {
      std::vector<std::string> columns;
      std::string cmd_args = args;

      boost::algorithm::trim(cmd_args);

      if (cmd_args.size())
        boost::algorithm::split(columns, cmd_args, boost::is_any_of(" "));

      std::vector<std::string>::iterator i = std::find(columns.begin(), columns.end(), "print-columnsinfo");
      const bool print_colinfo = i != columns.end();
      if (print_colinfo)
        columns.erase(i);

      result = context.connection()->recv_result();
      print_result_set(*result, columns, value_callback);

      if (print_colinfo)
        print_columndata(*result->columnMetadata());

      variables_to_unreplace.clear();
      int64_t x = result->affectedRows();
      if (x >= 0)
        std::cout << x << " rows affected\n";
      else
        std::cout << "command ok\n";
      if (result->lastInsertId() > 0)
        std::cout << "last insert id: " << result->lastInsertId() << "\n";
      if (!result->infoMessage().empty())
        std::cout << result->infoMessage() << "\n";
      {
        std::vector<mysqlx::Result::Warning> warnings(result->getWarnings());
        if (!warnings.empty())
          std::cout << "Warnings generated:\n";
        for (std::vector<mysqlx::Result::Warning>::const_iterator w = warnings.begin();
            w != warnings.end(); ++w)
        {
          std::cout << (w->is_note ? "NOTE" : "WARNING") << " | " << w->code << " | " << w->text << "\n";
        }
      }

      if (!OPT_expect_error->check_ok())
        return Stop_with_failure;
    }
    catch (mysqlx::Error &err)
    {
      if (result.get())
        result->mark_error();
      if (!OPT_expect_error->check_error(err))
        return Stop_with_failure;
    }
    return Continue;
  }

  Result cmd_recvuntil(Execution_context &context, const std::string &args)
  {
    int msgid;

    std::vector<std::string> argl;

    boost::split(argl, args, boost::is_any_of(" "), boost::token_compress_on);


    bool show = true, stop = false;

    if (argl.size() > 1)
      show = atoi(argl[1].c_str()) > 0;

    while (!stop)
    {
      boost::scoped_ptr<mysqlx::Message> msg(context.connection()->recv_raw(msgid));
      if (msg.get())
      {
        if (msg->GetDescriptor()->full_name() == argl[0] ||
            msgid == Mysqlx::ServerMessages::ERROR)
        {
          show = true;
          stop = true;
        }

        try
        {
          if (show)
            std::cout << message_to_text(*msg) << "\n";
        }
        catch (std::exception &e)
        {
          dumpx(e);
          if (OPT_fatal_errors)
            return Stop_with_success;
        }
      }
    }
    variables_to_unreplace.clear();
    return Continue;
  }

  Result cmd_enablessl(Execution_context &context, const std::string &args)
  {
    try
    {
      context.connection()->enable_tls();
    }
    catch (const mysqlx::Error &err)
    {
      dumpx(err);
      return Stop_with_failure;
    }

    return Continue;
  }

  Result cmd_stmt(Execution_context &context, const std::string &args)
  {
    Mysqlx::Sql::StmtExecute stmt;

    const bool is_sql = context.m_command_name.find("sql") != std::string::npos;
    std::string command = args;

    replace_variables(command);

    stmt.set_stmt(command);
    stmt.set_namespace_(is_sql ? "sql" : "xplugin");

    context.connection()->send(stmt);

    return Continue;
  }

  Result cmd_sleep(Execution_context &context, const std::string &args)
  {
    const float delay_in_seconds = atof(args.c_str());
#ifdef _WIN32
    const int delay_in_miliseconds = delay_in_seconds * 1000;
    Sleep(delay_in_miliseconds);
#else
    const int delay_in_ultraseconds = delay_in_seconds * 1000000;
    usleep(delay_in_ultraseconds);
#endif
    return Continue;
  }

  Result cmd_login(Execution_context &context, const std::string &args)
  {
    std::string user, pass, db, auth_meth;

    if (args.empty())
      context.m_cm->get_credentials(user, pass);
    else
    {
      std::string s = args;
      std::string::size_type p = s.find(CMD_ARG_SEPARATOR);
      if (p != std::string::npos)
      {
        user = s.substr(0, p);
        s = s.substr(p+1);
        p = s.find(CMD_ARG_SEPARATOR);
        if (p != std::string::npos)
        {
          pass = s.substr(0, p);
          s = s.substr(p+1);
          p = s.find(CMD_ARG_SEPARATOR);
          if (p != std::string::npos)
          {
            db = s.substr(0, p);
            auth_meth = s.substr(p+1);
          }
          else
            db = s;
        }
        else
          pass = s;
      }
      else
        user = s;
    }

    void (mysqlx::Connection::*method)(const std::string &, const std::string &, const std::string &);

    method = &mysqlx::Connection::authenticate_mysql41;

    try
    {
      context.connection()->push_local_notice_handler(boost::bind(dump_notices, _1, _2));
      //XXX
      // Prepered for method map
      if (0 == strncmp(auth_meth.c_str(), "plain", 5))
      {
        method = &mysqlx::Connection::authenticate_plain;
      }
      else if ( !(0 == strncmp(auth_meth.c_str(), "mysql41", 5) || 0 == auth_meth.length()))
        throw mysqlx::Error(CR_UNKNOWN_ERROR, "Wrong authentication method");

      (context.connection()->*method)(user, pass, db);

      context.connection()->pop_local_notice_handler();

      std::cout << "Login OK\n";
    }
    catch (mysqlx::Error &err)
    {
      context.connection()->pop_local_notice_handler();
      dumpx(err);
      if (OPT_fatal_errors)
        return Stop_with_success;
    }

    return Continue;
  }

  Result cmd_repeat(Execution_context &context, const std::string &args)
  {
    std::string variable_name = "";
    std::vector<std::string> argl;

    boost::split(argl, args, boost::is_any_of("\t"), boost::token_compress_on);

    if (argl.size() > 1)
    {
      variable_name = argl[1];
    }

    Loop_do loop = {context.m_stream.tellg(), atoi(argl[0].c_str()), 0, variable_name};

    m_loop_stack.push_back(loop);

    if (variable_name.length())
      variables[variable_name] = boost::lexical_cast<std::string>(loop.value);

    return Continue;
  }

  Result cmd_endrepeat(Execution_context &context, const std::string &args)
  {
    while (m_loop_stack.size())
    {
      Loop_do &ld = m_loop_stack.back();

      --ld.iterations;
      ++ld.value;

      if (ld.variable_name.length())
        variables[ld.variable_name] = boost::lexical_cast<std::string>(ld.value);

      if (1 > ld.iterations)
      {
        m_loop_stack.pop_back();
        break;
      }

      context.m_stream.seekg(ld.block_begin);
      break;
    }

    return Continue;
  }

  Result cmd_loginerror(Execution_context &context, const std::string &args)
  {
    std::string s = args;
    std::string expected, user, pass, db;
    std::string::size_type p = s.find('\t');
    if (p != std::string::npos)
    {
      expected = s.substr(0, p);
      s = s.substr(p+1);
      p = s.find('\t');
      if (p != std::string::npos)
      {
        user = s.substr(0, p);
        s = s.substr(p+1);
        p = s.find('\t');
        if (p != std::string::npos)
        {
          pass = s.substr(0, p+1);
          db = s.substr(p+1);
        }
        else
          pass = s;
      }
      else
        user = s;
    }
    else
    {
      std::cout << error() << "Missing arguments to -->loginerror" << eoerr();
      return Stop_with_failure;
    }
    try
    {
      context.connection()->push_local_notice_handler(boost::bind(dump_notices, _1, _2));

      context.connection()->authenticate_mysql41(user, pass, db);

      context.connection()->pop_local_notice_handler();

      std::cout << error() << "Login succeeded, but an error was expected" << eoerr();
      if (OPT_fatal_errors)
        return Stop_with_failure;
    }
    catch (mysqlx::Error &err)
    {
      context.connection()->pop_local_notice_handler();

      if (err.error() == (int32_t)atoi(expected.c_str()))
        std::cerr << "error (as expected): " << err.what() << " (code " << err.error() << ")\n";
      else
      {
        std::cerr << error() << "was expecting: " << expected << " but got: " << err.what() << " (code " << err.error() << ")" << eoerr();
        if (OPT_fatal_errors)
          return Stop_with_failure;
      }
    }

    return Continue;
  }

  Result cmd_system(Execution_context &context, const std::string &args)
  {
    // XXX - remove command
    // command used only at dev level
    // example of usage
    // -->system (sleep 3; echo "Killing"; ps aux | grep mysqld | egrep -v "gdb .+mysqld" | grep -v  "kdeinit4"| awk '{print($2)}' | xargs kill -s SIGQUIT)&
    if (0 == system(args.c_str()))
      return Continue;

    return Stop_with_failure;
  }

  Result cmd_recv_all_until_disc(Execution_context &context, const std::string &args)
  {
    int msgid;
    try
    {
      while(true)
      {
        boost::scoped_ptr<mysqlx::Message> msg(context.connection()->recv_raw(msgid));

        //TODO:
        // For now this command will be used in places where random messages
        // can reach mysqlxtest in different mtr rans
        // the random behavior of server in such cases should be fixed
        //if (msg.get())
        //  std::cout << unreplace_variables(message_to_text(*msg), true) << "\n";
      }
    }
    catch (mysqlx::Error&)
    {
      std::cerr << "Server disconnected\n";
    }

    if (context.m_cm->is_default_active())
      return Stop_with_success;

    context.m_cm->active()->set_closed();
    context.m_cm->close_active(false);

    return Continue;
  }

  Result cmd_peerdisc(Execution_context &context, const std::string &args)
  {
    int expected_delta_time;
    int tolerance;
    int result = sscanf(args.c_str(),"%i %i", &expected_delta_time, &tolerance);

    if (result <1 || result > 2)
    {
      std::cerr << "ERROR: Invalid use of command\n";

      return Stop_with_failure;
    }

    if (1 == result)
    {
      tolerance = 10 * expected_delta_time / 100;
    }

    boost::posix_time::ptime start_time = boost::posix_time::microsec_clock::local_time();
    try
    {
      int msgid;

      boost::scoped_ptr<mysqlx::Message> msg(context.connection()->recv_raw_with_deadline(msgid, 2 * expected_delta_time));

      if (msg.get())
      {
        std::cerr << "ERROR: Received unexpected message.\n";
        std::cerr << message_to_text(*msg) << "\n";
      }
      else
      {
        std::cerr << "ERROR: Timeout occur while waiting for disconnection.\n";
      }

      return Stop_with_failure;
    }
    catch (const mysqlx::Error &ec)
    {
      if (CR_SERVER_GONE_ERROR != ec.error())
      {
        dumpx(ec);
        return Stop_with_failure;
      }
    }

    int execution_delta_time = (boost::posix_time::microsec_clock::local_time() - start_time).total_milliseconds();

    if (abs(execution_delta_time - expected_delta_time) > tolerance)
    {
      std::cerr << "ERROR: Peer disconnected after: "<< execution_delta_time << "[ms], expected: " << expected_delta_time << "[ms]\n";
      return Stop_with_failure;
    }

    if (context.m_cm->is_default_active())
      return Stop_with_success;

    context.m_cm->active()->set_closed();
    context.m_cm->close_active(false);

    return Continue;
  }

  Result cmd_recv(Execution_context &context, const std::string &args)
  {
    int msgid;
    bool quiet = false;
    std::string args_copy(args);

    boost::algorithm::trim(args_copy);
    if (args_copy == "quiet")
      quiet = true;
    else if (!args_copy.empty())
    {
      std::cerr << "ERROR: Unknown command argument: " << args_copy << "\n";
      return Stop_with_failure;
    }

    try
    {
      boost::scoped_ptr<mysqlx::Message> msg(context.connection()->recv_raw(msgid));

      if (msg.get() && !quiet)
        std::cout << unreplace_variables(message_to_text(*msg), true) << "\n";
      if (!OPT_expect_error->check_ok())
        return Stop_with_failure;
    }
    catch (mysqlx::Error &e)
    {
      if (!quiet && !OPT_expect_error->check_error(e))
        return Stop_with_failure;
    }
    catch (std::exception &e)
    {
      std::cerr << "ERROR: "<< e.what()<<"\n";
      if (OPT_fatal_errors)
        return Stop_with_failure;
    }
    return Continue;
  }

  Result cmd_exit(Execution_context &context, const std::string &args)
  {
    return Stop_with_success;
  }

  Result cmd_abort(Execution_context &context, const std::string &args)
  {
    exit(2);
    return Stop_with_success;
  }

  Result cmd_nowarnings(Execution_context &context, const std::string &args)
  {
    OPT_show_warnings = false;
    return Continue;
  }

  Result cmd_yeswarnings(Execution_context &context, const std::string &args)
  {
    OPT_show_warnings = true;
    return Continue;
  }

  Result cmd_fatalerrors(Execution_context &context, const std::string &args)
  {
    OPT_fatal_errors = true;
    return Continue;
  }

  Result cmd_nofatalerrors(Execution_context &context, const std::string &args)
  {
    OPT_fatal_errors = false;
    return Continue;
  }

  Result cmd_newsessionplain(Execution_context &context, const std::string &args)
  {
    return do_newsession(context, args, true);
  }

  Result cmd_newsession(Execution_context &context, const std::string &args)
  {
    return do_newsession(context, args, false);
  }

  Result do_newsession(Execution_context &context, const std::string &args, bool plain)
  {
    std::string s = args;
    std::string user, pass, db, name;
    std::string::size_type p = s.find(CMD_ARG_SEPARATOR);
    if (p != std::string::npos)
    {
      name = s.substr(0, p);
      s = s.substr(p+1);
      p = s.find(CMD_ARG_SEPARATOR);
      if (p != std::string::npos)
      {
        user = s.substr(0, p);
        s = s.substr(p+1);
        p = s.find(CMD_ARG_SEPARATOR);
        if (p != std::string::npos)
        {
          pass = s.substr(0, p);
          db = s.substr(p+1);
        }
        else
          pass = s;
      }
      else
        user = s;
    }
    else
      name = s;

    try
    {
      context.m_cm->create(name, user, pass, db, plain);
      if (!OPT_expect_error->check_ok())
        return Stop_with_failure;
    }
    catch (mysqlx::Error &err)
    {
      if (!OPT_expect_error->check_error(err))
        return Stop_with_failure;
    }

    return Continue;
  }

  Result cmd_setsession(Execution_context &context, const std::string &args)
  {
    if (!args.empty() && (args[0] == ' ' || args[0] == '\t'))
      context.m_cm->set_active(args.substr(1));
    else
      context.m_cm->set_active(args);
    return Continue;
  }

  Result cmd_closesession(Execution_context &context, const std::string &args)
  {
    try
    {
      if (args == " abort")
        context.m_cm->abort_active();
      else
        context.m_cm->close_active();
      if (!OPT_expect_error->check_ok())
        return Stop_with_failure;
    }
    catch (mysqlx::Error &err)
    {
      if (!OPT_expect_error->check_error(err))
        return Stop_with_failure;
    }
    return Continue;
  }

  Result cmd_expecterror(Execution_context &context, const std::string &args)
  {
    if (!args.empty())
    {
      std::vector<std::string> argl;
      boost::split(argl, args, boost::is_any_of(","), boost::token_compress_on);
      for (std::vector<std::string>::const_iterator arg = argl.begin(); arg != argl.end(); ++arg)
        OPT_expect_error->expect_errno(atoi(arg->c_str()));
    }
    else
    {
      std::cerr << "expecterror requires an errno argument\n";
      return Stop_with_failure;
    }
    return Continue;
  }


  static boost::posix_time::ptime m_start_measure;

  Result cmd_measure(Execution_context &context, const std::string &args)
  {
    m_start_measure = boost::posix_time::microsec_clock::local_time();
    return Continue;
  }

  Result cmd_endmeasure(Execution_context &context, const std::string &args)
  {
    if (m_start_measure.is_not_a_date_time())
    {
      std::cerr << "Time measurement, wasn't initialized\n";
      return Stop_with_failure;
    }

    std::vector<std::string> argl;
    boost::split(argl, args, boost::is_any_of(" "), boost::token_compress_on);
    if (argl.size() != 2 && argl.size() != 1)
    {
      std::cerr << "Invalid number of arguments for command endmeasure\n";
      return Stop_with_failure;
    }

    const int64_t expected_msec = atoi(argl[0].c_str());
    const int64_t msec = (boost::posix_time::microsec_clock::local_time() - m_start_measure).total_milliseconds();

    int64_t tolerance = expected_msec * 10 / 100;

    if (2 == argl.size())
      tolerance = atoi(argl[1].c_str());

    if (abs(expected_msec - msec) > tolerance)
    {
      std::cerr << "Timeout should occur after " << expected_msec << "ms, but it was " << msec <<"ms.  \n";
      return Stop_with_failure;
    }

    m_start_measure = boost::posix_time::not_a_date_time;
    return Continue;
  }

  Result cmd_quiet(Execution_context &context, const std::string &args)
  {
    OPT_quiet = true;

    return Continue;
  }

  Result cmd_noquiet(Execution_context &context, const std::string &args)
  {
    OPT_quiet = false;

    return Continue;
  }

  Result cmd_varsub(Execution_context &context, const std::string &args)
  {
    variables_to_unreplace.push_back(args);
    return Continue;
  }

  Result cmd_varlet(Execution_context &context, const std::string &args)
  {
    std::string::size_type p = args.find(' ');
    if (p == std::string::npos)
    {
      if (variables.find(args) == variables.end())
      {
        std::cerr << "Invalid variable " << args << "\n";
        return Stop_with_failure;
      }
      variables.erase(args);
    }
    else
    {
      std::string value = args.substr(p+1);
      replace_variables(value);
      variables[args.substr(0, p)] = value;
    }
    return Continue;
  }

  Result cmd_varinc(Execution_context &context, const std::string &args)
  {
    std::vector<std::string> argl;
    boost::split(argl, args, boost::is_any_of(" "), boost::token_compress_on);
    if (argl.size() != 2)
    {
      std::cerr << "Invalid number of arguments for command varinc\n";
      return Stop_with_failure;
    }

    if (variables.find(argl[0]) == variables.end())
    {
      std::cerr << "Invalid variable " << argl[0] << "\n";
      return Stop_with_failure;
    }

    std::string val = variables[argl[0]];
    char* c;
    long int_val = strtol(val.c_str(), &c, 10);
    long int_n = strtol(argl[1].c_str(), &c, 10);
    int_val += int_n;
    val = boost::lexical_cast<std::string>(int_val);
    variables[argl[0]] = val;

    return Continue;
  }

  Result cmd_vargen(Execution_context &context, const std::string &args)
  {
    std::vector<std::string> argl;
    boost::split(argl, args, boost::is_any_of(" "), boost::token_compress_on);
    if (argl.size() != 3)
    {
      std::cerr << "Invalid number of arguments for command vargen\n";
      return Stop_with_failure;
    }
    std::string data(atoi(argl[2].c_str()), *argl[1].c_str());
    variables[argl[0]] = data;
    return Continue;
  }

  Result cmd_varfile(Execution_context &context, const std::string &args)
  {
    std::vector<std::string> argl;
    boost::split(argl, args, boost::is_any_of(" "), boost::token_compress_on);
    if (argl.size() != 2)
    {
      std::cerr << "Invalid number of arguments for command varfile " << args << "\n";
      return Stop_with_failure;
    }

    std::ifstream file(argl[1].c_str());
    if (!file.is_open())
    {
      std::cerr << "Coult not open file " << argl[1]<<"\n";
      return Stop_with_failure;
    }

    file.seekg(0, file.end);
    size_t len = file.tellg();
    file.seekg(0);

    char *buffer = new char[len];
    file.read(buffer, len);
    variables[argl[0]] = std::string(buffer, len);
    delete []buffer;

    return Continue;
  }

  Result cmd_binsend(Execution_context &context, const std::string &args)
  {
    std::string args_copy = args;
    replace_variables(args_copy);
    std::string data = bindump_to_data(args_copy);

    std::cout << "Sending " << data.length() << " bytes raw data...\n";
    context.m_cm->active()->send_bytes(data);
    return Continue;
  }

  Result cmd_hexsend(Execution_context &context, const std::string &args)
  {
    std::string args_copy = args;
    replace_variables(args_copy);

    if (0 == args_copy.length())
    {
      std::cerr << "Data should not be present\n";
      return Stop_with_failure;
    }

    if (0 != args_copy.length() % 2)
    {
      std::cerr << "Size of data should be a multiplication of two, current length:" << args_copy.length()<<"\n";
      return Stop_with_failure;
    }

    std::string data;
    try
    {
      boost::algorithm::unhex(args_copy.begin(), args_copy.end(), std::back_inserter(data));
    }
    catch(const std::exception&)
    {
      std::cerr << "Hex string is invalid\n";
      return Stop_with_failure;
    }

    std::cout << "Sending " << data.length() << " bytes raw data...\n";
    context.m_cm->active()->send_bytes(data);
    return Continue;
  }

  size_t value_to_offset(const std::string &data, const size_t maximum_value)
  {
    if ('%' == *data.rbegin())
    {
      size_t percent = atoi(data.c_str());

      return maximum_value * percent / 100;
    }

    return atoi(data.c_str());
  }

  Result cmd_binsendoffset(Execution_context &context, const std::string &args)
  {
    std::string args_copy = args;
    replace_variables(args_copy);

    std::vector<std::string> argl;
    boost::split(argl, args_copy, boost::is_any_of(" "), boost::token_compress_on);

    size_t begin_bin = 0;
    size_t end_bin = 0;
    std::string data;

    try
    {
      data = bindump_to_data(argl[0]);
      end_bin = data.length();

      if (argl.size() > 1)
      {
        begin_bin = value_to_offset(argl[1], data.length());
        if (argl.size() > 2)
        {
          end_bin = value_to_offset(argl[2], data.length());

          if (argl.size() > 3)
            throw std::out_of_range("Too many arguments");
        }
      }
    }
    catch (const std::out_of_range&)
    {
      std::cerr << "Invalid number of arguments for command binsendoffset:" << argl.size() << "\n";
      return Stop_with_failure;
    }

    std::cout << "Sending " << end_bin << " bytes raw data...\n";
    context.m_cm->active()->send_bytes(data.substr(begin_bin, end_bin - begin_bin));
    return Continue;
  }

  Result cmd_callmacro(Execution_context &context, const std::string &args)
  {
    if (Macro::call(context, args))
      return Continue;
    return Stop_with_failure;
  }

  Result cmd_import(Execution_context &context, const std::string &args);
};

boost::posix_time::ptime Command::m_start_measure = boost::posix_time::not_a_date_time;

static int process_client_message(mysqlx::Connection *connection, int8_t msg_id, const mysqlx::Message &msg)
{
  if (!OPT_quiet)
    std::cout << "send " << message_to_text(msg) << "\n";

  if (OPT_bindump)
    std::cout << message_to_bindump(msg) << "\n";

  try
  {
    // send request
    connection->send(msg_id, msg);

    if (!OPT_expect_error->check_ok())
      return 1;
  }
  catch (mysqlx::Error &err)
  {
    if (!OPT_expect_error->check_error(err))
      return 1;
  }
  return 0;
}

static void print_result_set(mysqlx::Result &result)
{
  std::vector<std::string> empty_column_array_print_all;

  print_result_set(result, empty_column_array_print_all);
}

template<typename T>
std::string get_object_value(const T &value)
{
  std::stringstream result;
  result << value;

  return result.str();
}

std::string get_field_value(boost::shared_ptr<mysqlx::Row> &row, const int field, boost::shared_ptr<std::vector<mysqlx::ColumnMetadata> > &meta)
{
  if (row->isNullField(field))
  {
    return "null";
  }

  try
  {
    const mysqlx::ColumnMetadata &col(meta->at(field));

    switch (col.type)
    {
    case mysqlx::SINT:
      return get_object_value(row->sInt64Field(field));

    case mysqlx::UINT:
      return get_object_value(row->uInt64Field(field));

    case mysqlx::DOUBLE:
      if (col.fractional_digits >= 31)
      {
        char buffer[100];
        my_gcvt(row->doubleField(field), MY_GCVT_ARG_DOUBLE, sizeof(buffer)-1, buffer, NULL);
        return buffer;
      }
      else
      {
        char buffer[100];
        my_fcvt(row->doubleField(field), col.fractional_digits, buffer, NULL);
        return buffer;
      }

    case mysqlx::FLOAT:
      if (col.fractional_digits >= 31)
      {
        char buffer[100];
        my_gcvt(row->floatField(field), MY_GCVT_ARG_FLOAT, sizeof(buffer)-1, buffer, NULL);
        return buffer;
      }
      else
      {
        char buffer[100];
        my_fcvt(row->floatField(field), col.fractional_digits, buffer, NULL);
        return buffer;
      }

    case mysqlx::BYTES:
    {
      std::string tmp(row->stringField(field));
      return unreplace_variables(tmp, false);
    }

    case mysqlx::TIME:
      return get_object_value(row->timeField(field));

    case mysqlx::DATETIME:
      return get_object_value(row->dateTimeField(field));

    case mysqlx::DECIMAL:
      return row->decimalField(field);

    case mysqlx::SET:
      return row->setFieldStr(field);

    case mysqlx::ENUM:
      return row->enumField(field);

    case mysqlx::BIT:
      return get_object_value(row->bitField(field));
    }
  }
  catch (std::exception &e)
  {
    std::cout << "ERROR: " << e.what() << "\n";
  }

  return "";
}


namespace
{

inline std::string get_typename(const mysqlx::FieldType& field)
{
  switch (field)
  {
  case mysqlx::SINT:
    return "SINT";
  case mysqlx::UINT:
    return "UINT";
  case mysqlx::DOUBLE:
    return "DOUBLE";
  case mysqlx::FLOAT:
    return "FLOAT";
  case mysqlx::BYTES:
    return "BYTES";
  case mysqlx::TIME:
    return "TIME";
  case mysqlx::DATETIME:
    return "DATETIME";
  case mysqlx::SET:
    return "SET";
  case mysqlx::ENUM:
    return "ENUM";
  case mysqlx::BIT:
    return "BIT";
  case mysqlx::DECIMAL:
    return "DECIMAL";
  }
  return "UNKNOWN";
}


inline std::string get_flags(const mysqlx::FieldType& field, uint32_t flags)
{
  std::string r;

  if (flags & MYSQLX_COLUMN_FLAGS_UINT_ZEROFILL) // and other equal 1
  {
    switch (field)
    {
    case mysqlx::SINT:
    case mysqlx::UINT:
      r += " ZEROFILL";
      break;

    case mysqlx::DOUBLE:
    case mysqlx::FLOAT:
    case mysqlx::DECIMAL:
      r += " UNSIGNED";
      break;

    case mysqlx::BYTES:
      r += " RIGHTPAD";
      break;

    case mysqlx::DATETIME:
      r += " TIMESTAMP";
      break;

    default:
      ;
    }
  }
  if (flags & MYSQLX_COLUMN_FLAGS_NOT_NULL)
    r += " NOT_NULL";

  if (flags & MYSQLX_COLUMN_FLAGS_PRIMARY_KEY)
    r += " PRIMARY_KEY";

  if (flags & MYSQLX_COLUMN_FLAGS_UNIQUE_KEY)
    r += " UNIQUE_KEY";

  if (flags & MYSQLX_COLUMN_FLAGS_MULTIPLE_KEY)
    r += " MULTIPLE_KEY";

  if (flags & MYSQLX_COLUMN_FLAGS_AUTO_INCREMENT)
    r += " AUTO_INCREMENT";

  return r;
}


} // namespace


static void print_columndata(const std::vector<mysqlx::ColumnMetadata> &meta)
{
  for (std::vector<mysqlx::ColumnMetadata>::const_iterator col = meta.begin(); col != meta.end(); ++col)
  {
    std::cout << col->name << ":" << get_typename(col->type) << ':'
              << get_flags(col->type, col->flags) << '\n';
  }
}

static void print_result_set(mysqlx::Result &result, const std::vector<std::string> &columns, Value_callback value_callback)
{
  boost::shared_ptr<std::vector<mysqlx::ColumnMetadata> > meta(result.columnMetadata());
  std::vector<int> column_indexes;
  int column_index = -1;
  bool first = true;

  for (std::vector<mysqlx::ColumnMetadata>::const_iterator col = meta->begin();
      col != meta->end(); ++col)
  {
    ++column_index;

    if (!first)
      std::cout << "\t";
    else
      first = false;

    if (!columns.empty() && columns.end() == std::find(columns.begin(), columns.end(), col->name))
      continue;

    column_indexes.push_back(column_index);
    std::cout << col->name;
  }
  std::cout << "\n";

  for (;;)
  {
    boost::shared_ptr<mysqlx::Row> row(result.next());
    if (!row.get())
      break;

    std::vector<int>::iterator i = column_indexes.begin();
    for (; i != column_indexes.end() && (*i) < row->numFields(); ++i)
    {
      int field = (*i);
      if (field != 0)
        std::cout << "\t";

      std::string result = get_field_value(row, field, meta);

      if (value_callback)
      {
        value_callback(result);
        value_callback.clear();
      }
      std::cout << result;
    }
    std::cout << "\n";
  }
}

static int run_sql_batch(mysqlx::Connection *conn, const std::string &sql_)
{
  std::string delimiter = ";";
  std::vector<std::pair<size_t, size_t> > ranges;
  std::stack<std::string> input_context_stack;
  std::string sql = sql_;

  replace_variables(sql);

  shcore::mysql::splitter::determineStatementRanges(sql.data(), sql.length(), delimiter,
                                                    ranges, "\n", input_context_stack);

  for (std::vector<std::pair<size_t, size_t> >::const_iterator st = ranges.begin(); st != ranges.end(); ++st)
  {
    try
    {
      if (!OPT_quiet)
        std::cout << "RUN " << sql.substr(st->first, st->second) << "\n";
      boost::shared_ptr<mysqlx::Result> result(conn->execute_sql(sql.substr(st->first, st->second)));
      if (result.get())
      {
        do
        {
          print_result_set(*result.get());
        } while (result->nextDataSet());

        int64_t x = result->affectedRows();
        if (x >= 0)
          std::cout << x << " rows affected\n";
        if (result->lastInsertId() > 0)
          std::cout << "last insert id: " << result->lastInsertId() << "\n";
        if (!result->infoMessage().empty())
          std::cout << result->infoMessage() << "\n";

        if (OPT_show_warnings)
        {
          std::vector<mysqlx::Result::Warning> warnings(result->getWarnings());
          if (!warnings.empty())
            std::cout << "Warnings generated:\n";
          for (std::vector<mysqlx::Result::Warning>::const_iterator w = warnings.begin();
              w != warnings.end(); ++w)
          {
            std::cout << (w->is_note ? "NOTE" : "WARNING") << " | " << w->code << " | " << w->text << "\n";
          }
        }
      }
    }
    catch (mysqlx::Error &err)
    {
      variables_to_unreplace.clear();

      std::cerr << "While executing " << sql.substr(st->first, st->second) << ":\n";
      if (!OPT_expect_error->check_error(err))
        return 1;
    }
  }
  variables_to_unreplace.clear();
  return 0;
}

enum Block_result
{
  Block_result_feed_more,
  Block_result_eated_but_not_hungry,
  Block_result_not_hungry,
  Block_result_indigestion,
  Block_result_everyone_not_hungry
};

class Block_processor
{
public:
  virtual ~Block_processor() {}

  virtual Block_result feed(std::istream &input, const char *linebuf) = 0;
  virtual bool feed_ended_is_state_ok() { return true; }
};

typedef boost::shared_ptr<Block_processor> Block_processor_ptr;

class Sql_block_processor : public Block_processor
{
public:
  Sql_block_processor(Connection_manager *cm)
  : m_cm(cm), m_sql(false)
  { }

  virtual Block_result feed(std::istream &input, const char *linebuf)
  {
    if (m_sql)
    {
      if (strcmp(linebuf, "-->endsql") == 0)
      {
        {
          int r = run_sql_batch(m_cm->active(), m_rawbuffer);
          if (r != 0)
          {
            return Block_result_indigestion;
          }
        }
        m_sql = false;

        return Block_result_eated_but_not_hungry;
      }
      else
        m_rawbuffer.append(linebuf).append("\n");

      return Block_result_feed_more;
    }

    // -->command
    if (strcmp(linebuf, "-->sql") == 0)
    {
      m_rawbuffer.clear();
      m_sql = true;
      // feed everything until -->endraw to the mysql client

      return Block_result_feed_more;
    }

    return Block_result_not_hungry;
  }

  virtual bool feed_ended_is_state_ok()
  {
    if (m_sql)
    {
      std::cerr << error() << "Unclosed -->sql directive" << eoerr();
      return false;
    }

    return true;
  }

private:
  Connection_manager *m_cm;
  std::string m_rawbuffer;
  bool m_sql;
};

class Macro_block_processor : public Block_processor
{
public:
  Macro_block_processor(Connection_manager *cm)
  : m_cm(cm)
  { }

  ~Macro_block_processor()
  {
  }

  virtual Block_result feed(std::istream &input, const char *linebuf)
  {
    if (m_macro)
    {
      if (strcmp(linebuf, "-->endmacro") == 0)
      {
        m_macro->set_body(m_rawbuffer);

        Macro::add(m_macro);
        if (OPT_verbose)
          std::cout << "Macro " << m_macro->name() << " defined\n";

        m_macro.reset();

        return Block_result_eated_but_not_hungry;
      }
      else
        m_rawbuffer.append(linebuf).append("\n");

      return Block_result_feed_more;
    }

    // -->command
    const char *cmd = "-->macro ";
    if (strncmp(linebuf, cmd, strlen(cmd)) == 0)
    {
      std::list<std::string> args;
      std::string t(linebuf+strlen(cmd));
      boost::split(args, t, boost::is_any_of(" \t"), boost::token_compress_on);

      if (args.empty())
      {
        std::cerr << error() << "Missing macro name argument for -->macro" << eoerr();
        return Block_result_indigestion;
      }

      m_rawbuffer.clear();
      std::string name = args.front();
      args.pop_front();
      m_macro.reset(new Macro(name, args));

      return Block_result_feed_more;
    }

    return Block_result_not_hungry;
  }

  virtual bool feed_ended_is_state_ok()
  {
    if (m_macro)
    {
      std::cerr << error() << "Unclosed -->macro directive" << eoerr();
      return false;
    }

    return true;
  }

private:
  Connection_manager *m_cm;
  boost::shared_ptr<Macro> m_macro;
  std::string m_rawbuffer;
};

class Single_command_processor: public Block_processor
{
public:
  Single_command_processor(Connection_manager *cm)
  : m_cm(cm)
  { }

  virtual Block_result feed(std::istream &input, const char *linebuf)
  {
    Execution_context context(input, m_cm);

    if (m_command.is_command_syntax(linebuf))
    {
      {
        Command::Result r = m_command.process(context, linebuf);
        if (Command::Stop_with_failure == r)
          return Block_result_indigestion;
        else if (Command::Stop_with_success == r)
          return Block_result_everyone_not_hungry;
      }

      return Block_result_eated_but_not_hungry;
    }
    // # comment
    else if (linebuf[0] == '#' || linebuf[0] == 0)
    {
      return Block_result_eated_but_not_hungry;
    }

    return Block_result_not_hungry;
  }

private:
  Command m_command;
  Connection_manager *m_cm;
};

class Snd_message_block_processor: public Block_processor
{
public:
  Snd_message_block_processor(Connection_manager *cm)
  : m_cm(cm)
  { }

  virtual Block_result feed(std::istream &input, const char *linebuf)
  {
    if (m_full_name.empty())
    {
      if (!(m_full_name = get_message_name(linebuf)).empty())
      {
        m_buffer.clear();
        return Block_result_feed_more;
      }
    }
    else
    {
      if (linebuf[0] == '}')
      {
        int8_t msg_id;
        std::string processed_buffer = m_buffer;
        replace_variables(processed_buffer);

        boost::scoped_ptr<mysqlx::Message> msg(text_to_client_message(m_full_name, processed_buffer, msg_id));

        m_full_name.clear();
        if (!msg.get())
          return Block_result_indigestion;

        {
          int r = process(msg_id, *msg.get());

          if (r != 0)
            return Block_result_indigestion;
        }

        return Block_result_eated_but_not_hungry;
      }
      else
      {
        m_buffer.append(linebuf).append("\n");
        return Block_result_feed_more;
      }
    }

    return Block_result_not_hungry;
  }

  virtual bool feed_ended_is_state_ok()
  {
    if (!m_full_name.empty())
    {
      std::cerr << error() << "Incomplete message " << m_full_name << eoerr();
      return false;
    }

    return true;
  }

private:
  virtual std::string get_message_name(const char *linebuf)
  {
    const char *p;
    if ((p = strstr(linebuf, " {")))
    {
      return std::string(linebuf, p-linebuf);
    }

    return "";
  }

  virtual int process(const int8_t msg_id, mysqlx::Message &message)
  {
    return process_client_message(m_cm->active(), msg_id, message);
  }

  Connection_manager *m_cm;
  std::string m_buffer;
  std::string m_full_name;
};

class Dump_message_block_processor: public Snd_message_block_processor
{
public:
  Dump_message_block_processor(Connection_manager *cm)
  : Snd_message_block_processor(cm)
  { }

private:
  virtual std::string get_message_name(const char *linebuf)
  {
    const char *command_dump = "-->binparse";
    std::vector<std::string> args;

    boost::split(args, linebuf, boost::is_any_of(" "), boost::token_compress_on);

    if (4 != args.size())
      return "";

    if (args[0] == command_dump && args[3] == "{")
    {
      m_variable_name = args[1];
      return args[2];
    }

    return "";
  }

  virtual int process(const int8_t msg_id, mysqlx::Message &message)
  {
    std::string bin_message = message_to_bindump(message);

    variables[m_variable_name] = bin_message;

    return 0;
  }

  std::string m_variable_name;
};

static int process_client_input(std::istream &input, std::vector<Block_processor_ptr> &eaters)
{
  const std::size_t buffer_length = 64*1024 + 1024;
  char              linebuf[buffer_length + 1];

  linebuf[buffer_length] = 0;

  if (!input.good())
  {
    std::cerr << "Input stream isn't valid\n";

    return 1;
  }

  Block_processor_ptr hungry_block_reader;

  while (!input.eof())
  {
    Block_result result = Block_result_not_hungry;

    input.getline(linebuf, buffer_length);
    script_stack.front().line_number++;

    if (!hungry_block_reader)
    {
      std::vector<Block_processor_ptr>::iterator i = eaters.begin();

      while (i != eaters.end() &&
          Block_result_not_hungry == result)
      {
        result = (*i)->feed(input, linebuf);

        if (Block_result_indigestion == result)
          return 1;

        if (Block_result_feed_more == result)
          hungry_block_reader = (*i);

        ++i;
      }

      if (Block_result_everyone_not_hungry == result)
        break;

      continue;
    }

    result = hungry_block_reader->feed(input, linebuf);

    if (Block_result_indigestion == result)
      return 1;

    if (Block_result_feed_more != result)
      hungry_block_reader.reset();

    if (Block_result_everyone_not_hungry == result)
      break;
  }

  std::vector<Block_processor_ptr>::iterator i = eaters.begin();

  while (i != eaters.end())
  {
    if (!(*i)->feed_ended_is_state_ok())
      return 1;

    ++i;
  }

  return 0;
}

#include "cmdline_options.h"

class My_command_line_options : public Command_line_options
{
public:
  enum Run_mode{
    RunTest,
    RunTestWithoutAuth
  } run_mode;

  std::string run_file;
  bool        has_file;
  bool        cap_expired_password;
  bool        dont_wait_for_server_disconnect;
  bool        use_plain_auth;

  int port;
  int timeout;
  std::string host;
  std::string uri;
  std::string password;
  std::string schema;
  mysqlx::Ssl_config ssl;
  bool        daemon;

  void print_help()
  {
    std::cout << "mysqlxtest <options>\n";
    std::cout << "Options:\n";
    std::cout << "-f, --file=<file>     Reads input from file\n";
    std::cout << "-n, --no-auth         Skip authentication which is required by -->sql block (run mode)\n";
    std::cout << "--plain-auth          Use PLAIN text authentication mechanism\n";
    std::cout << "-u, --user=<user>a    Connection user\n";
    std::cout << "-p, --password=<pass> Connection password\n";
    std::cout << "-h, --host=<host>     Connection host\n";
    std::cout << "-P, --port=<port>     Connection port\n";
    std::cout << "-t, --timeout=<ms>    Connection timeout\n";
    std::cout << "--close-no-sync       Do not wait for connection to be closed by server(disconnect first)\n";
    std::cout << "--schema=<schema>     Default schema to connect to\n";
    std::cout << "--uri=<uri>           Connection URI\n";
    std::cout << "--ssl-key             X509 key in PEM format\n";
    std::cout << "--ssl-ca              CA file in PEM format\n";
    std::cout << "--ssl-ca_path         CA directory\n";
    std::cout << "--ssl-cert            X509 cert in PEM format\n";
    std::cout << "--ssl-cipher          SSL cipher to use\n";
    std::cout << "--connect-expired-password Allow expired password\n";
    std::cout << "--quiet               Don't print out messages sent\n";
    std::cout << "--fatal-errors=<0|1>  Mysqlxtest is started with ignoring or stopping on fatal error\n";
    std::cout << "-B, --bindump         Dump binary representation of messages sent, in format suitable for\n";
    std::cout << "--verbose             Enable extra verbose messages\n";
    std::cout << "--daemon              Work as a daemon (unix only)\n";
    std::cout << "--help                Show command line help\n";
    std::cout << "--help-commands       Show help for input commands\n";
    std::cout << "\nOnly one option that changes run mode is allowed.\n";
  }

  void print_help_commands()
  {
    std::cout << "Input may be a file (or if no --file is specified, it stdin will be used)\n";
    std::cout << "The following commands may appear in the input script:\n";
    std::cout << "-->echo <text>\n";
    std::cout << "  Prints the text (allows variables)\n";
    std::cout << "-->title <c><text>\n";
    std::cout << "  Prints the text with an underline, using the character <c>\n";
    std::cout << "-->sql\n";
    std::cout << "  Begins SQL block. SQL statements that appear will be executed and results printed (allows variables).\n";
    std::cout << "-->endsql\n";
    std::cout << "  End SQL block. End a block of SQL started by -->sql\n";
    std::cout << "-->macro <macroname> <argname1> ...\n";
    std::cout << "  Start a block of text to be defined as a macro. Must be terminated with -->endmacro\n";
    std::cout << "-->endmacro\n";
    std::cout << "  Ends a macro block\n";
    std::cout << "-->callmacro <macro>\t<argvalue1>\t...\n";
    std::cout << "  Executes the macro text, substituting argument values with the provided ones (args separated by tabs).\n";
    std::cout << "-->import <macrofile>\n";
    std::cout << "  Loads macros from the specified file\n";
    std::cout << "-->enablessl\n";
    std::cout << "  Enables ssl on current connection\n";
    std::cout << "<protomsg>\n";
    std::cout << "  Encodes the text format protobuf message and sends it to the server (allows variables).\n";
    std::cout << "-->recv [quiet]\n";
    std::cout << "  Read and print (if not quiet) one message from the server\n";
    std::cout << "-->recvresult [print-columnsinfo]\n";
    std::cout << "  Read and print one resultset from the server; if print-columnsinfo is present also print short columns status\n";
    std::cout << "-->recvtovar <varname>\n";
    std::cout << "  Read and print one resultset from the server and sets the variable from first row\n";
    std::cout << "-->recverror <errno>\n";
    std::cout << "  Read a message and ensure that it's an error of the expected type\n";
    std::cout << "-->recvtype <msgtype>\n";
    std::cout << "  Read one message and print it, checking that its type is the specified one\n";
    std::cout << "-->recvuntil <msgtype>\n";
    std::cout << "  Read messages and print them, until a msg of the specified type (or Error) is received\n";
    std::cout << "-->repeat <N>\n";
    std::cout << "  Begin block of instructions that should be repeated N times\n";
    std::cout << "-->endrepeat\n";
    std::cout << "  End block of instructions that should be repeated - next iteration\n";
    std::cout << "-->stmtsql <CMD>\n";
    std::cout << "  Send StmtExecute with sql command\n";
    std::cout << "-->stmtadmin <CMD>\n";
    std::cout << "  Send StmtExecute with admin command\n";
    std::cout << "-->system <CMD>\n";
    std::cout << "  Execute application or script (dev only)\n";
    std::cout << "-->exit\n";
    std::cout << "  Stops reading commands, disconnects and exits (same as <eof>/^D)\n";
    std::cout << "-->abort\n";
    std::cout << "  Exit immediately, without performing cleanup\n";
    std::cout << "-->nowarnings/-->yeswarnings\n";
    std::cout << "   Whether to print warnings generated by the statement (default no)\n";
    std::cout << "-->peerdisc <MILISECONDS> [TOLERANCE]\n";
    std::cout << "  Expect that xplugin disconnects after given number of milliseconds and tolerance\n";
    std::cout << "-->sleep <SECONDS>\n";
    std::cout << "  Stops execution of mysqxtest for given number of seconds (may be fractional)\n";
    std::cout << "-->login <user>\t<pass>\t<db>\t<mysql41|plain>]\n";
    std::cout << "  Performs authentication steps (use with --no-auth)\n";
    std::cout << "-->loginerror <errno>\t<user>\t<pass>\t<db>\n";
    std::cout << "  Performs authentication steps expecting an error (use with --no-auth)\n";
    std::cout << "-->fatalerrors/nofatalerrors\n";
    std::cout << "  Whether to immediately exit on MySQL errors\n";
    std::cout << "-->expecterror <errno>\n";
    std::cout << "  Expect a specific errof for the next command and fail if something else occurs\n";
    std::cout << "  Works for: newsession, closesession, recvresult\n";
    std::cout << "-->newsession <name>\t<user>\t<pass>\t<db>\n";
    std::cout << "  Create a new connection with given name and account (use - as user for no-auth)\n";
    std::cout << "-->newsessionplain <name>\t<user>\t<pass>\t<db>\n";
    std::cout << "  Create a new connection with given name and account and force it to NOT use ssl, even if its generally enabled\n";
    std::cout << "-->setsession <name>\n";
    std::cout << "  Activate the named session\n";
    std::cout << "-->closesession [abort]\n";
    std::cout << "  Close the active session (unless its the default session)\n";
    std::cout << "-->varfile <varname> <datafile>\n";
    std::cout << "   Assigns the contents of the file to the named variable\n";
    std::cout << "-->varlet <varname> <value>\n";
    std::cout << "   Assign the value (can be another variable) to the variable\n";
    std::cout << "-->varinc <varname> <n>\n";
    std::cout << "   Increment the value of varname by n (assuming both convert to integral)\n";
    std::cout << "-->varsub <varname>\n";
    std::cout << "   Add a variable to the list of variables to replace for the next recv or sql command (value is replaced by the name)\n";
    std::cout << "-->binsend <bindump>[<bindump>...]\n";
    std::cout << "   Sends one or more binary message dumps to the server (generate those with --bindump)\n";
    std::cout << "-->binsendoffset <srcvar> [offset-begin[percent]> [offset-end[percent]]]\n";
    std::cout << "   Same as binsend with begin and end offset of data to be send\n";
    std::cout << "-->binparse MESSAGE.NAME {\n";
    std::cout << "    MESSAGE.DATA\n";
    std::cout << "}\n";
    std::cout << "   Dump given message to variable %MESSAGE_DUMP%\n";
    std::cout << "-->quiet/noquiet\n";
    std::cout << "   Toggle verbose messages\n";
    std::cout << "# comment\n";
  }

  bool set_mode(Run_mode mode)
  {
    if (RunTest != run_mode)
      return false;

    run_mode = mode;

    return true;
  }

  My_command_line_options(int argc, char **argv)
  : Command_line_options(argc, argv), run_mode(RunTest), has_file(false),
    cap_expired_password(false), dont_wait_for_server_disconnect(false),
    use_plain_auth(false), port(0), timeout(0l), daemon(false)
  {
    std::string user;

    run_mode = RunTest; // run tests by default

    for (int i = 1; i < argc && exit_code == 0; i++)
    {
      char *value;
      if (check_arg_with_value(argv, i, "--file", "-f", value))
      {
        run_file = value;
        has_file = true;
      }
      else if (check_arg(argv, i, "--no-auth", "-n"))
      {
        if (!set_mode(RunTestWithoutAuth))
        {
          std::cerr << "Only one option that changes run mode is allowed.\n";
          exit_code = 1;
        }
      }
      else if (check_arg(argv, i, "--plain-auth", NULL))
      {
        use_plain_auth = true;
      }
      else if (check_arg_with_value(argv, i, "--password", "-p", value))
        password = value;
      else if (check_arg_with_value(argv, i, "--ssl-key", NULL, value))
        ssl.key = value;
      else if (check_arg_with_value(argv, i, "--ssl-ca", NULL, value))
        ssl.ca = value;
      else if (check_arg_with_value(argv, i, "--ssl-ca_path", NULL, value))
        ssl.ca_path = value;
      else if (check_arg_with_value(argv, i, "--ssl-cert", NULL, value))
        ssl.cert = value;
      else if (check_arg_with_value(argv, i, "--ssl-cipher", NULL, value))
        ssl.cipher = value;
      else if (check_arg_with_value(argv, i, "--host", "-h", value))
        host = value;
      else if (check_arg_with_value(argv, i, "--user", "-u", value))
        user = value;
      else if (check_arg_with_value(argv, i, "--schema", NULL, value))
        schema = value;
      else if (check_arg_with_value(argv, i, "--port", "-P", value))
        port = atoi(value);
      else if (check_arg_with_value(argv, i, "--timeout", "-t", value))
        timeout = atoi(value);
      else if (check_arg_with_value(argv, i, "--fatal-errors", NULL, value))
        OPT_fatal_errors = atoi(value);
      else if (check_arg_with_value(argv, i, "--password", "-p", value))
        password = value;
      else if (check_arg_with_value(argv, i, NULL, "-v", value))
        set_variable_option(value);
      else if (check_arg(argv, i, "--close-no-sync", NULL))
        dont_wait_for_server_disconnect = true;
      else if (check_arg(argv, i, "--bindump", "-B"))
        OPT_bindump = true;
      else if (check_arg(argv, i, "--connect-expired-password", NULL))
        cap_expired_password = true;
      else if (check_arg(argv, i, "--quiet", "-q"))
        OPT_quiet = true;
      else if (check_arg(argv, i, "--verbose", "-v"))
        OPT_verbose = true;
      else if (check_arg(argv, i, "--daemon", NULL))
        daemon = true;
#ifndef _WIN32
      else if (check_arg(argv, i, "--color", NULL))
        OPT_color = true;
#endif
      else if (check_arg(argv, i, "--help", "--help"))
      {
        print_help();
        exit_code = 1;
      }
      else if (check_arg(argv, i, "--help-commands", "--help-commands"))
      {
        print_help_commands();
        exit_code = 1;
      }
      else if (exit_code == 0)
      {
        std::cerr << argv[0] << ": unknown option " << argv[i] << "\n";
        exit_code = 1;
        break;
      }
    }

    if (port == 0)
      port = 33060;
    if (host.empty())
      host = "localhost";

    if (uri.empty())
    {
      uri = user;
      if (!uri.empty()) {
        if (!password.empty()) {
          uri.append(":").append(password);
        }
        uri.append("@");
      }
      uri.append(host);
      {
        char buf[10];
        my_snprintf(buf, sizeof(buf), ":%i", port);
        uri.append(buf);
      }
      if (!schema.empty())
        uri.append("/").append(schema);
    }
  }

  void set_variable_option(const std::string &set_expression)
  {
    std::vector<std::string> args;

    boost::algorithm::split(args, set_expression, boost::is_any_of("="));

    if (2 != args.size())
    {
      std::cerr << "Wrong format expected NAME=VALUE\n";
      exit_code = 1;
      return;
    }

    variables[args[0]] = args[1];
  }
};

std::vector<Block_processor_ptr> create_macro_block_processors(Connection_manager *cm)
{
  std::vector<Block_processor_ptr> result;

  result.push_back(boost::make_shared<Sql_block_processor>(cm));
  result.push_back(boost::make_shared<Dump_message_block_processor>(cm));
  result.push_back(boost::make_shared<Single_command_processor>(cm));
  result.push_back(boost::make_shared<Snd_message_block_processor>(cm));

  return result;
}

std::vector<Block_processor_ptr> create_block_processors(Connection_manager *cm)
{
  std::vector<Block_processor_ptr> result;

  result.push_back(boost::make_shared<Sql_block_processor>(cm));
  result.push_back(boost::make_shared<Macro_block_processor>(cm));
  result.push_back(boost::make_shared<Dump_message_block_processor>(cm));
  result.push_back(boost::make_shared<Single_command_processor>(cm));
  result.push_back(boost::make_shared<Snd_message_block_processor>(cm));

  return result;
}

static int process_client_input_on_session(const My_command_line_options &options, std::istream &input)
{
  Connection_manager cm(options.uri, options.ssl, options.timeout, options.dont_wait_for_server_disconnect);
  int r = 1;

  try
  {
    std::vector<Block_processor_ptr> eaters;

    cm.connect_default(options.cap_expired_password, options.use_plain_auth);
    eaters = create_block_processors(&cm);
    r = process_client_input(input, eaters);
    cm.close_active(true);
  }
  catch (mysqlx::Error &error)
  {
    dumpx(error);
    std::cerr << "not ok\n";
    return 1;
  }

  if (r == 0)
    std::cerr << "ok\n";
  else
    std::cerr << "not ok\n";

  return r;
}

static int process_client_input_no_auth(const My_command_line_options &options, std::istream &input)
{
  Connection_manager cm(options.uri, options.ssl, options.timeout, options.dont_wait_for_server_disconnect);
  int r = 1;

  try
  {
    std::vector<Block_processor_ptr> eaters;

    cm.active()->set_closed();
    eaters = create_block_processors(&cm);
    r = process_client_input(input, eaters);
  }
  catch (mysqlx::Error &error)
  {
    dumpx(error);
    std::cerr << "not ok\n";
    return 1;
  }

  if (r == 0)
    std::cerr << "ok\n";
  else
    std::cerr << "not ok\n";

  return r;
}

bool Macro::call(Execution_context &context, const std::string &cmd)
{
  std::string name;
  std::string macro = get(cmd, name);
  if (macro.empty())
    return false;

  Stack_frame frame = {0, "macro "+name};
  script_stack.push_front(frame);

  std::stringstream stream(macro);
  std::vector<Block_processor_ptr> processors(create_macro_block_processors(context.m_cm));

  bool r = process_client_input(stream, processors) == 0;

  script_stack.pop_front();

  return r;
}

Command::Result Command::cmd_import(Execution_context &context, const std::string &args)
{
  std::ifstream fs;
  fs.open(args.c_str());
  if (!fs.good())
  {
    std::cerr << error() << "Could not open macro file " << args << eoerr();
    return Stop_with_failure;
  }

  Stack_frame frame = {0, args};
  script_stack.push_front(frame);

  std::vector<Block_processor_ptr> processors;
  processors.push_back(boost::make_shared<Macro_block_processor>(context.m_cm));
  bool r = process_client_input(fs, processors) == 0;
  script_stack.pop_front();

  return r ? Continue : Stop_with_failure;
}

typedef int (*Program_mode)(const My_command_line_options &, std::istream &input);

static std::istream &get_input(My_command_line_options &opt, std::ifstream &file)
{
  if (opt.has_file)
  {
    file.open(opt.run_file.c_str());

    if (!file.is_open())
    {
      std::cerr << "ERROR: Could not open file " << opt.run_file << "\n";
      opt.exit_code = 1;
    }

    return file;
  }

  return std::cin;
}


inline void unable_daemonize()
{
  std::cerr << "ERROR: Unable to put process in background\n";
  exit(2);
}


void daemonize()
{
#ifdef WIN32
  unable_daemonize();
#else
  if (getppid() == 1) // already a daemon
    exit(0);
  pid_t pid = fork();
  if (pid < 0)
    unable_daemonize();
  if (pid > 0)
    exit(0);
  if (setsid() < 0)
    unable_daemonize();
#endif
}


static Program_mode get_mode_function(const My_command_line_options &opt)
{
  switch(opt.run_mode)
  {
  case My_command_line_options::RunTestWithoutAuth:
    return process_client_input_no_auth;

  case My_command_line_options::RunTest:
  default:
    return process_client_input_on_session;
  }
}

int main(int argc, char **argv)
{
  OPT_expect_error = new Expected_error();
  My_command_line_options options(argc, argv);

  if (options.exit_code != 0)
    return options.exit_code;

  if (options.daemon)
    daemonize();

  std::cout << std::unitbuf;
  std::ifstream fs;
  std::istream &input = get_input(options, fs);
  Program_mode  mode  = get_mode_function(options);

#ifdef WIN32
  WSADATA wsaData;
  if (WSAStartup(MAKEWORD(1, 1), &wsaData) != 0)
  {
    std::cerr << "WSAStartup failed\n";
    return 1;
  }
#endif

  ssl_start();

  bool result = 0;
  try
  {
    Stack_frame frame = {0, "main"};
    script_stack.push_front(frame);

    result = mode(options, input);
  }
  catch (std::exception &e)
  {
    std::cerr << "ERROR: " << e.what() << "\n";
    result = 1;
  }

  vio_end();
  return result;
}

#include "mysqlx_all_msgs.h"

#ifdef _MSC_VER
#  pragma pop_macro("ERROR")
#endif
