{-# LANGUAGE OverloadedStrings #-}

import Control.Lens                                 hiding ( (.=) )
import Control.Concurrent ( threadDelay )
import Control.Monad

import qualified Data.Text                          as T
import           Data.Aeson
import qualified Data.Map                           as M
import qualified Data.ByteString.Char8              as C8
import qualified Data.ByteString.Lazy               as BL
import           Network.Wreq

import           Haskakafka
import           Haskakafka.InternalRdKafkaEnum

data CampaignTopicMessage = CampaignTopicMessage { id :: Int,
                                                   src :: T.Text
                                                 } deriving (Show)

instance FromJSON CampaignTopicMessage where
 parseJSON (Object v) =
    CampaignTopicMessage <$> v .: "id"
                         <*> v .: "src"
 parseJSON _ = mzero

api :: String -> String
api s = "http://portal.bitcodin.com/api/" ++ s

opts :: Options
opts = defaults & header "bitcodin-api-key" .~ ["3d03c4648b4b6170e7ad7986b637ddcd26a6053a49eb2aa25ec01961a4dd3e2d"]

createInput :: T.Text -> IO (Response BL.ByteString)
createInput src = do
    let payload = encode $ object ["inputType" .= ("url" :: T.Text), "url" .= src]
    postWith opts (api "/input/create") payload

decodeCTPayload :: C8.ByteString -> Maybe CampaignTopicMessage
decodeCTPayload p = decode $ BL.fromStrict p

handleConsume :: Either KafkaError KafkaMessage -> IO ()
handleConsume e = do
    case e of
      (Left err) -> case err of
                      KafkaResponseError RdKafkaRespErrTimedOut -> return ()
                      _                                         -> putStrLn $ "[ERROR] " ++ (show err)
      (Right m) -> do
          print $ BL.fromStrict $ messagePayload m
          case decodeCTPayload $ messagePayload m of
            Nothing -> putStrLn "[ERROR] decode campaignInput"
            Just m -> do
              handleResponse =<< (createInput $ src m)

handleResponse :: Response BL.ByteString -> IO ()
handleResponse r = do
    putStrLn $ show $ r ^. responseStatus . statusCode


main :: IO ()
main = do
    let partition = 0
        host = "localhost:9092"
        topic = "campaignRequest"
        kafkaConfig = []
        topicConfig = []
    withKafkaConsumer kafkaConfig topicConfig
                      host topic
                      partition
                      KafkaOffsetStored
                      $ \kafka topic -> forever $ do
    handleConsume =<< consumeMessage topic partition 1000
    --threadDelay 100000 -- 10 times a second


  --r <- mapM createInput srcs
  --print r
