dofile("table_show.lua")
dofile("urlcode.lua")
local urlparse = require("socket.url")
local luasocket = require("socket") -- Used to get sub-second time
local http = require("socket.http")
JSON = assert(loadfile "JSON.lua")()

local item_name_newline = os.getenv("item_name_newline")
local item_dir = os.getenv('item_dir')
local warc_file_base = os.getenv('warc_file_base')

local url_count = 0
local tries = 0
local downloaded = {}
local addedtolist = {}
local abortgrab = false


discovered_items = {}

last_main_site_time = 0


if urlparse == nil or http == nil then
  io.stdout:write("socket not corrently installed.\n")
  io.stdout:flush()
  abortgrab = true
end

for ignore in io.open("ignore-list", "r"):lines() do
  downloaded[ignore] = true
  if string.match(ignore, '^https:') then
    downloaded[string.gsub(ignore, '^https', 'http', 1)] = true
  elseif string.match(ignore, '^http:') then
    downloaded[string.gsub(ignore, '^http:', 'https:', 1)] = true
  end
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

do_debug = false
print_debug = function(a)
    if do_debug then
        print(a)
    end
end
print_debug("The grab script is running in debug mode. You should not see this in production.")

allowed = function(url, parenturl)
  -- Do not recurse to other item base URLs

  -- page-less e.g. "type=first_holder" have "force" set on check, so this will not block them
  if string.match(url, "https?://supermariomakerbookmark%.nintendo%.net/profile/") and not string.match(url, "page=") then
    username = string.match(url, "https?://supermariomakerbookmark%.nintendo%.net/profile/([^%?&]+)")
    p_assert(string.match(username, "^[^/ ]+$"))
    if not string.match(item_name_newline, username) then
      discovered_items["user:" .. username] = true
    end
    return false
  end
  if string.match(url, "https?://supermariomakerbookmark%.nintendo%.net/courses/") then
    course_name = string.match(url, "https?://supermariomakerbookmark%.nintendo%.net/courses/([^%?&/]+)")
    p_assert(string.match(course_name, "^[A-F0-9][A-F0-9][A-F0-9][A-F0-9]%-[A-F0-9][A-F0-9][A-F0-9][A-F0-9]%-[A-F0-9][A-F0-9][A-F0-9][A-F0-9]%-[A-F0-9][A-F0-9][A-F0-9][A-F0-9]$"))
    if not string.match(item_name_newline, course_name) then
      discovered_items["course:" .. course_name] = true
    end
    return false
  end
  
  -- Only get course images on course pages, and profile images on profile pages
  if string.match(url, "^https?://dypqnhofrd2x2%.cloudfront%.net/.*jpg$") then -- Course images
    p_assert(parenturl)
    if string.match(parenturl, "^https?://supermariomakerbookmark%.nintendo%.net/profile/") then
      print_debug("Rejecting " .. url .. " because it came from a profile page")
      return false
    end
  elseif string.match(url, "^https?://mii%-secure%.cdn%.nintendo%.net/.*png$") then -- Profile images
    p_assert(parenturl)
    if not string.match(parenturl, "^https?://mii%-secure%.cdn%.nintendo%.net/.*png$") then
      print_debug("Rejecting " .. url .. " because it did not come from another image.")
      return false
    end
  end
  
  
  -- General restrictions
  if string.match(url, "^https?://supermariomakerbookmark%.nintendo%.net/[^/]*$")
    or string.match(url, "^https?://www%.googletagmanager%.com")
    or string.match(url, "^https?://supermariomakerbookmark%.nintendo%.net/users/") -- Auth
    or string.match(url, "^https?://supermariomakerbookmark%.nintendo%.net/assets/") -- Static
    or string.match(url, "^https?://www%.esrb%.org/") then
    return false
  end
  
  -- b64 that gets picked up somewhere
  if string.match(url, "==$") then
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
      
  return true
end

wget.callbacks.download_child_p = function(urlpos, parent, depth, start_url_parsed, iri, verdict, reason)
  local url = urlpos["url"]["url"]
  if downloaded[url] == true or addedtolist[url] == true then
    return false
  end
  if allowed(url, parent["url"]) then
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
    p_assert((not force) or (force == true)) -- Don't accidentally put something else for force
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
  
  local function load_html()
    if html == nil then
      html = read_file(file)
    end
  end
  
  
  if string.match(url, "https?://supermariomakerbookmark%.nintendo%.net/profile/[^/%?]+$") and status_code == 200 then
    check(url .. "?type=posted", true)
    check(url .. "?type=liked", true)
    check(url .. "?type=fastest_holder", true)
    check(url .. "?type=first_holder", true)
    
    load_html()
    
    p_assert(string.match(html, "Play History"))
    
    profile_image = string.match(html, '<img class="mii" src="(https?://mii%-secure%.cdn%.nintendo%.net/[^"]*png)" alt="[^>]+" />')
    p_assert(profile_image)
    check(profile_image, true)
  end
  
  if string.match(url, "https?://supermariomakerbookmark%.nintendo%.net/course/") and status_code == 200 then
    load_html()
    p_assert(string.match(html, "Course Tag"))
  end
  
  -- Queue alternate profile picture types
  if string.match(url, "https?://mii%-secure%.cdn%.nintendo%.net/.*png$") then
    check((string.gsub(url, "normal", "like")))
    check((string.gsub(url, "like", "normal")))
    print_debug("Queuing alternate face from " .. url)
  end

  

  if status_code == 200 and not (string.match(url, "jpe?g$") or string.match(url, "png$")) then
    load_html()
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
  
  
  if status_code == 403 and (string.match(url["url"], "^https?://supermariomakerbookmark%.nintendo%.net/")) then
    print("You have been banned from the Super Mario Maker website. Switch your worker to another project, and wait a few hours to be unbanned.")
    print("This should not happen if you are running one concurrency per IP address. If it does, tell OrIdow6 in the project channel.")
    os.execute("sleep " .. 60) -- Do not spam the tracker (or the site)
    return wget.actions.ABORT
  end
  
  --
  
  if string.match(url["url"], "^https?://supermariomakerbookmark%.nintendo%.net") then
    -- Sleep for up to 2s average
    local now_t = luasocket.gettime()
    local makeup_time = 2 - (now_t - last_main_site_time)
    if makeup_time > 0 then
      print_debug("Sleeping for main site " .. makeup_time)
      os.execute("sleep " .. makeup_time)
    end
    last_main_site_time = now_t
  end
  
  --

  
  local do_retry = false
  local maxtries = 12
  local url_is_essential = false
  
  if status_code == 0
    or (status_code > 400 and status_code ~= 404) then
    io.stdout:write("Server returned " .. http_stat.statcode .. " (" .. err .. "). Sleeping.\n")
    io.stdout:flush()
    do_retry = true
  end
  
  --url_is_essential = string.match(url, "https?://supermariomakerbookmark%.nintendo%.net/profile/[^\?]+$")
  -- or string.match(url, "https?://supermariomakerbookmark%.nintendo%.net/courses/")
  url_is_essential = true -- For now, all URLS, including CDN urls, are considered such
  
  
  if do_retry then
    if tries >= maxtries then
      io.stdout:write("I give up...\n")
      io.stdout:flush()
      tries = 0
      if not url_is_essential then
        return wget.actions.EXIT
      else
        print("Failed on an essential URL, aborting...")
        return wget.actions.ABORT
      end
    else
      sleep_time = math.floor(math.pow(2, tries))
      tries = tries + 1
    end
  end


  if do_retry and sleep_time > 0.001 then
    print("Sleeping " .. sleep_time .. "s")
    os.execute("sleep " .. sleep_time)
    return wget.actions.CONTINUE
  end
  
  tries = 0
  return wget.actions.NOTHING
end


wget.callbacks.finish = function(start_time, end_time, wall_time, numurls, total_downloaded_bytes, total_download_time)
  if do_debug then
    for item, _ in pairs(discovered_items) do
      print("Would have sent discovered item " .. item)
    end
  else
    to_send = nil
    for item, _ in pairs(discovered_items) do
      if to_send == nil then
        to_send = url
      else
        to_send = to_send .. "\0" .. item
      end
    end

    if to_send ~= nil then
      local tries = 0
      while tries < 10 do
        local body, code, headers, status = http.request(
          "http://blackbird-amqp.meo.ws:23038/whatever/",
          to_send
        )
        if code == 200 or code == 409 then
          break
        end
        os.execute("sleep " .. math.floor(math.pow(2, tries)))
        tries = tries + 1
      end
      if tries == 10 then
        abortgrab = true
      end
    end
  end
end


wget.callbacks.before_exit = function(exit_status, exit_status_string)
  if abortgrab == true then
    return wget.exits.IO_FAIL
  end
  return exit_status
end

