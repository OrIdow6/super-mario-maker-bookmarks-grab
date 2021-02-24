dofile("table_show.lua")
dofile("urlcode.lua")
local urlparse = require("socket.url")
local http = require("socket.http")
JSON = assert(loadfile "JSON.lua")()

local item_value = os.getenv('item_value')
local item_dir = os.getenv('item_dir')
local warc_file_base = os.getenv('warc_file_base')

local url_count = 0
local tries = 0
local downloaded = {}
local addedtolist = {}
local abortgrab = false



-- Keep track of what's been POSTed to http://nmsg.nicovideo.jp/api
local post_queue = {}
local post_current = nil
local post_finished = {}

local user_id = nil



if urlparse == nil or http == nil then
  io.stdout:write("socket not corrently installed.\n")
  io.stdout:flush()
  abortgrab = true
end

for ignore in io.open("ignore-list", "r"):lines() do
  downloaded[ignore] = true
  downloaded[string.gsub(ignore, '^https', 'http', 1)] = true
end

read_file = function(file)
  if file then
    local f = assert(io.open(file))
    local data = f:read("*all")
    f:close()
    return data
  else
    return ""
  end
end

io.stdout:setvbuf("no") -- So prints are not buffered - http://lua.2524044.n2.nabble.com/print-stdout-and-flush-td6406981.html

p_assert = function(v)
  if not v then
    print("Assertion failed - aborting item")
    print(debug.traceback())
    abortgrab = true
  end
end

allowed = function(url, parenturl)
  -- Do not queue any more watch pages
  -- Also prevents it from picking up many junk links that are on these pages for some reason
  if string.match(url, "^https?://www%.nicovideo%.jp/watch/") then
    return false
  end
  
  
  if string.match(url, "^https?://account%.nicovideo%.jp")
  or (string.match(url, "^https?://nicovideo%.cdn%.nimg%.jp")
      and not string.match(url, "nicoaccount"))
  or string.match(url, "^https?://m%.nicovideo%.jp")
  or string.match(url, "^https?://embed%.nicovideo%.jp")
  or string.match(url, "^https?://qa%.nicovideo%.jp")
  or string.match(url, "^https?://secure%.nicovideo%.jp") 
  or string.match(url, "^https?://[^/]googletagmanager")
  or string.match(url, "^https?://[^/]+%.ads%.nicovideo%.jp")
  or string.match(url, "^https?://ads%.nicovideo%.jp")
  or string.match(url, "^https?://nmsg%.nicovideo%.jp")
  or string.match(url, "^https?://www%.nicovideo%.jp/video_top/")
  or string.match(url, "^https?://www%.nicovideo%.jp/tag/")
  or string.match(url, "^https?://dic%.nicovideo%.jp/")
  or string.match(url, "^https?://sp%.nicovideo%.jp/")then
    return false
  end

  local tested = {}
  for s in string.gmatch(url, "([^/]+)") do
    if tested[s] == nil then
      tested[s] = 0
    end
    if tested[s] == 6 then
      return false
    end
    tested[s] = tested[s] + 1
  end
  
  if string.match(url, "^https?://[^/]+%.nicovideo%.jp") then
    return true
  end

  --#return false
end

wget.callbacks.download_child_p = function(urlpos, parent, depth, start_url_parsed, iri, verdict, reason)
  local url = urlpos["url"]["url"]
  if downloaded[url] == true or addedtolist[url] == true then
    return false
  end
  if allowed(url) then
    addedtolist[url] = true
    return true
  end

  return false
end

wget.callbacks.get_urls = function(file, url, is_css, iri)
  local urls = {}
  local html = nil

  downloaded[url] = true

  local function check(urla, force)
    local origurl = url
    local url = string.match(urla, "^([^#]+)")
    local url_ = string.match(url, "^(.-)%.?$")
    url_ = string.gsub(url_, "&amp;", "&")
    url_ = string.match(url_, "^(.-)%s*$")
    url_ = string.match(url_, "^(.-)%??$")
    url_ = string.match(url_, "^(.-)&?$")
    url_ = string.match(url_, "^(.-)/?$")
    if (downloaded[url_] ~= true and addedtolist[url_] ~= true)
      and (allowed(url_, origurl) or force) then
      table.insert(urls, { url=url_ })
      addedtolist[url_] = true
      addedtolist[url] = true
    end
  end

  local function checknewurl(newurl)
    if string.match(newurl, "\\[uU]002[fF]") then
      return checknewurl(string.gsub(newurl, "\\[uU]002[fF]", "/"))
    end
    if string.match(newurl, "^https?:////") then
      check(string.gsub(newurl, ":////", "://"))
    elseif string.match(newurl, "^https?://") then
      check(newurl)
    elseif string.match(newurl, "^https?:\\/\\?/") then
      check(string.gsub(newurl, "\\", ""))
    elseif string.match(newurl, "^\\/") then
      checknewurl(string.gsub(newurl, "\\", ""))
    elseif string.match(newurl, "^//") then
      check(urlparse.absolute(url, newurl))
    elseif string.match(newurl, "^/") then
      check(urlparse.absolute(url, newurl))
    elseif string.match(newurl, "^%.%./") then
      if string.match(url, "^https?://[^/]+/[^/]+/") then
        check(urlparse.absolute(url, newurl))
      else
        checknewurl(string.match(newurl, "^%.%.(/.+)$"))
      end
    elseif string.match(newurl, "^%./") then
      check(urlparse.absolute(url, newurl))
    end
  end

  local function checknewshorturl(newurl)
    if string.match(newurl, "^%?") then
      check(urlparse.absolute(url, newurl))
    elseif not (string.match(newurl, "^https?:\\?/\\?//?/?")
      or string.match(newurl, "^[/\\]")
      or string.match(newurl, "^%./")
      or string.match(newurl, "^[jJ]ava[sS]cript:")
      or string.match(newurl, "^[mM]ail[tT]o:")
      or string.match(newurl, "^vine:")
      or string.match(newurl, "^android%-app:")
      or string.match(newurl, "^ios%-app:")
      or string.match(newurl, "^%${")) then
      check(urlparse.absolute(url, "/" .. newurl))
    end
  end
  
  if string.match(url, "^https?://www%.nicovideo%.jp/watch/") and status_code == 200 then
    html = read_file(file)
    -- Georestricted video
    if string.match(html, 'された地域と同じ地域からのみ視聴できます') or string.match(html, '国からは視聴できません') then
      print("Video is georestricted - aborting.") -- This is not a debug print, do not remove
      abortgrab = true
    end
    -- Misc errors that give 200s (e.g. vid:sm8 (deleted by administrator))
    if not string.match(html, '<link href="https://nicovideo%.cdn%.nimg%.jp/web/styles/bundle/pages_watch_WatchExceptionPage%.css') then
      -- These scripts are essential for playback, but their URLs (specifically the hex at the end) are unstable
      for url in string.gmatch(html, 'src="([^"]+)"') do
        if string.match(url, "watch_dll.+%.js$") or string.match(url, "watch_app.+%.js$") then
          check(url, true)
        end
      end
      
      profile_picture = string.match(html, "(https:\\/\\/secure%-dcdn%.cdn%.nimg%.jp\\/nicoaccount\\/usericon\\/[0-9]+\\/[0-9]+%.[a-z]+%?[0-9]+)")
      if not string.match(html, "(https:\\/\\/secure%-dcdn%.cdn%.nimg%.jp\\/nicoaccount\\/usericon\\/defaults\\/blank.jpg)") then -- Don't want this, obviously
        p_assert(profile_picture)
        
        profile_picture = string.gsub(profile_picture, "\\/", "/")
        check(profile_picture, true)
      end
      
      -- Now start the comment downloading process
      --user_id = string.match(html, "user.user_id = '([0-9]+)'")
      user_id = "118049508"
      thread_id_section = string.match(html, "threadIds&quot(.+)&quot;tags")
      p_assert(thread_id_section)
      for thread_id in string.gmatch(thread_id_section, "[0-9][0-9][0-9]+") do
        check("https://flapi.nicovideo.jp/api/getwaybackkey?thread=" .. thread_id)
      end
    end
  end
  
  local function nextpost()
    post_finished[post_current] = true
    post_current = table.remove(post_queue)
    table.insert(urls, { url="http://nmsg.nicovideo.jp/api", post_data=post_current,headers={["Content-Type"]="text/plain"} })
    print(post_current) -- DEBUG
  end
    
  local function addpost(data)
    if post_finished[data] == true or data == post_current then
      return
    end
    table.insert(post_queue, data)
    if post_current == nil then
      post_current = table.remove(post_queue)
      if post_current ~= nil then
        table.insert(urls, { url="http://nmsg.nicovideo.jp/api", post_data=post_current, headers={["Content-Type"]="text/plain"}})
        print(post_current) -- DEBUG
      end
    end
  end
  
  if string.match(url, "^https?://flapi.nicovideo.jp/api/getwaybackkey") and status_code == 200 then
    html = read_file(file)
    print(html) -- DEBUG
    thread_id = string.match(url, "thread=([0-9]+)$")
    waybackkey = string.match(html, "waybackkey=(.+)$")
    p_assert(thread_id)
    p_assert(waybackkey)
    -- Japanese
    addpost('<thread thread="' .. thread_id .. '" version="20061206" res_from="1" when="1700000000" waybackkey="' .. waybackkey .. '" user_id="' .. user_id .. '"/>')
    -- English
    addpost('<thread thread="' .. thread_id .. '" version="20061206" res_from="1" when="1700000000" waybackkey="' .. waybackkey .. '" user_id="' .. user_id .. '" language="1"/>')
    -- Chinese
    addpost('<thread thread="' .. thread_id .. '" version="20061206" res_from="1" when="1700000000" waybackkey="' .. waybackkey .. '" user_id="' .. user_id .. '" language="2"/>')
  end
  
  if url == "http://nmsg.nicovideo.jp/api" and status_code == 200 then
    html = read_file(file)
    -- Queue the previous block of comments, if not yet at the end
    min_date = nil
    for date in string.gmatch(html, ' date="([0-9][0-9][0-9][0-9]+)"') do
      i = tonumber(date)
      if min_date == nil or i < min_date then
        min_date = i
      end
    end
    
    if min_date ~= nil then
      -- when is noninclusive, so this will not include the min_date comment
      addpost(string.gsub(post_current, ' when="[0-9]+"', ' when="' .. tostring(min_date) .. '"'))
    end
    
    nextpost()
  elseif url == "http://nmsg.nicovideo.jp/api" then -- DEBUG
    print(read_file(file))
    print(post_current)
  end
  

  

  -- T
  if allowed(url, nil) and status_code == 200 then
    html = read_file(file)
    for newurl in string.gmatch(string.gsub(html, "&quot;", '"'), '([^"]+)') do
      checknewurl(newurl)
    end
    for newurl in string.gmatch(string.gsub(html, "&#039;", "'"), "([^']+)") do
      checknewurl(newurl)
    end
    for newurl in string.gmatch(html, ">%s*([^<%s]+)") do
      checknewurl(newurl)
    end
    for newurl in string.gmatch(html, "[^%-]href='([^']+)'") do
      checknewshorturl(newurl)
    end
    for newurl in string.gmatch(html, '[^%-]href="([^"]+)"') do
      checknewshorturl(newurl)
    end
    for newurl in string.gmatch(html, ":%s*url%(([^%)]+)%)") do
      checknewurl(newurl)
    end
  end

  return urls
end

wget.callbacks.httploop_result = function(url, err, http_stat)
  status_code = http_stat["statcode"]

  url_count = url_count + 1
  io.stdout:write(url_count .. "=" .. status_code .. " " .. url["url"] .. "  \n")
  io.stdout:flush()

  if status_code >= 300 and status_code <= 399 then
    local newloc = urlparse.absolute(url["url"], http_stat["newloc"])
    if downloaded[newloc] == true or addedtolist[newloc] == true
      or not allowed(newloc, url["url"]) then
      tries = 0
      return wget.actions.EXIT
    end
  end

  if status_code >= 200 and status_code <= 399 then
    downloaded[url["url"]] = true
  end

  if abortgrab == true then
    io.stdout:write("ABORTING...\n")
    io.stdout:flush()
    return wget.actions.ABORT
  end

  
  if status_code == 0
    or (status_code > 400 and status_code ~= 404) then
    io.stdout:write("Server returned " .. http_stat.statcode .. " (" .. err .. "). Sleeping.\n")
    io.stdout:flush()
    local maxtries = 12
    if not (allowed(url["url"], nil) or string.match(url["url"], "^https?://www%.nicovideo%.jp/watch/")) then
      maxtries = 3
    end
    if tries >= maxtries then
      io.stdout:write("I give up...\n")
      io.stdout:flush()
      tries = 0
      if maxtries == 3 then
        return wget.actions.EXIT
      else
        return wget.actions.ABORT
      end
    else
      if string.match(url["url"], "^https?://www%.nicovideo%.jp/watch/") and status_code == 503 then
        -- Their version of a 429
        os.execute("sleep 60")
      else
        os.execute("sleep " .. math.floor(math.pow(2, tries)))
      end
      tries = tries + 1
      return wget.actions.CONTINUE
    end
  end

  tries = 0

  local sleep_time = 0

  if sleep_time > 0.001 then
    os.execute("sleep " .. sleep_time)
  end

  return wget.actions.NOTHING
end


wget.callbacks.before_exit = function(exit_status, exit_status_string)
  if abortgrab == true then
    return wget.exits.IO_FAIL
  end
  return exit_status
end

