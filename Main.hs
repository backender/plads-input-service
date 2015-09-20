{-# LANGUAGE OverloadedStrings #-}


import           Database.MySQL.Simple
import           Database.MySQL.Simple.QueryResults
import           Database.MySQL.Simple.Result

import           Control.Lens                       hiding ((.=))
import Control.Concurrent ( threadDelay )
import Control.Monad

import qualified Data.Text                          as T
import           Data.Aeson
import qualified Data.ByteString.Char8              as C8
import qualified Data.ByteString.Lazy               as BL
import           Network.Wreq

import           Haskakafka
import           Haskakafka.InternalRdKafkaEnum

data CampaignInfo = CampaignInfo { cid :: Int,
                                   src :: T.Text,
                                   mpd :: Maybe T.Text
                                 } deriving (Show)


instance QueryResults CampaignInfo where
    convertResults [fa,fb,fc] [va,vb,vc] =
            CampaignInfo a b c
        where a = convert fa va
              b = convert fb vb
              c = convert fc vc
    convertResults fs vs  = convertError fs vs 3


findCampaignInfo :: Connection -> IO [CampaignInfo]
findCampaignInfo conn = query_ conn
        "select c.id, m.src, m.mpd_url from campaign c left join media m on m.id = c.media_id"

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
  conn <- connect defaultConnectInfo {  connectPassword = "password",
                                        connectDatabase = "plads"
                                     }
  c <- findCampaignInfo conn
  let srcs = map src $ filter (\x -> mpd x == Nothing) c

  let partition = 0
      host = "localhost:9092"
      topic = "test"
      kafkaConfig = []
      topicConfig = []
  withKafkaConsumer kafkaConfig topicConfig
                    host topic
                    partition
                    KafkaOffsetStored
                    $ \kafka topic -> do
  forever $ do
      let timeoutMs = 1000
      me <- consumeMessage topic partition timeoutMs
      case me of
        (Left err) -> case err of
                        KafkaResponseError RdKafkaRespErrTimedOut -> return ()
                        _                      -> putStrLn $ "[ERROR] " ++ (show err)
        (Right m) -> putStrLn $ "Woo, payload was " ++ (C8.unpack $ messagePayload m)
      threadDelay 1000000


  --r <- mapM createInput srcs
  --print r

  return ()
