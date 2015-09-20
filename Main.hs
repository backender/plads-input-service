{-# LANGUAGE OverloadedStrings #-}

import           Control.Monad

import           Database.MySQL.Simple
import           Database.MySQL.Simple.QueryResults
import           Database.MySQL.Simple.Result

import           Data.DateTime
import qualified Data.Text                          as T

import           Control.Lens                       hiding ((.=))
import           Data.Aeson
import qualified Data.ByteString.Lazy               as BL
import           Network.Wreq

data User = User { uid      :: Int,
                   active   :: Int,
                   salt     :: T.Text,
                   email    :: T.Text,
                   password :: T.Text,
                   roles    :: Maybe T.Text
                  } deriving (Show)

data CampaignInfo = CampaignInfo { cid     :: Int,
                                   display :: Int,
                                   start   :: DateTime,
                                   end     :: DateTime,
                                   src     :: T.Text,
                                   mpd     :: Maybe T.Text
                                 } deriving (Show)


instance QueryResults User where
    convertResults [fa,fb,fc,fd,fe,ff] [va,vb,vc,vd,ve,vf] =
            User id active salt email password roles
        where id = convert fa va
              active = convert fb vb
              salt = convert fc vc
              email = convert fd vd
              password = convert fe ve
              roles = convert ff vf
    convertResults fs vs  = convertError fs vs 6

instance QueryResults CampaignInfo where
    convertResults [fa,fb,fc,fd,fe,ff] [va,vb,vc,vd,ve,vf] =
            CampaignInfo cid display start end src mpd
        where cid = convert fa va
              display = convert fb vb
              start = convert fc vc
              end = convert fd vd
              src = convert fe ve
              mpd = convert ff vf
    convertResults fs vs  = convertError fs vs 6


findAllUser :: Connection -> IO [User]
findAllUser conn = query_ conn "select * from user"

findCampaignInfo :: Connection -> IO [CampaignInfo]
findCampaignInfo conn = query_ conn
        "select c.id, c.display_id, c.start, c.end, m.src, m.mpd_url from campaign c left join media m on m.id = c.media_id"

api :: String -> String
api s = "http://portal.bitcodin.com/api/" ++ s

opts :: Options
opts = defaults & header "bitcodin-api-key" .~ ["3d03c4648b4b6170e7ad7986b637ddcd26a6053a49eb2aa25ec01961a4dd3e2d"]

createInput :: T.Text -> IO (Response BL.ByteString)
createInput src = do
    let payload = encode $ object ["inputType" .= ("url" :: T.Text), "url" .= src]
    postWith opts (api "/input/create") payload


main :: IO ()
main = do
  conn <- connect defaultConnectInfo {
                                        connectPassword = "password",
                                        connectDatabase = "plads"
                                     }
  --users <- findAllUser conn
  --putStrLn $ show users

  c <- findCampaignInfo conn
  let srcs = map src $ filter (\x -> mpd x == Nothing) c

  r <- mapM createInput srcs
  print r

  return ()
