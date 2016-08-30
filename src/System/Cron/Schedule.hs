{-# LANGUAGE CPP                        #-}
{-# LANGUAGE DeriveFunctor              #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE ScopedTypeVariables        #-}
{-# LANGUAGE FlexibleContexts           #-}
--------------------------------------------------------------------
-- |
-- Module      : System.Cron.Schedule
-- Description : Monad stack for scheduling jobs to be executed by cron rules.
-- Copyright   : (c) Andrew Rademacher 2014
-- License     : MIT
--
-- Maintainer: Andrew Rademacher <andrewrademacher@gmail.com>
-- Portability: portable
--
-- > main :: IO ()
-- > main = do
-- >        ...
-- >        tids <- execSchedule $ do
-- >            addJob job1 "* * * * *"
-- >            addJob job2 "0 * * * *"
-- >        print tids
-- >        ...
-- >
-- > job1 :: IO ()
-- > job1 = putStrLn "Job 1"
-- >
-- > job2 :: IO ()
-- > job2 = putStrLn "Job 2"
--
--------------------------------------------------------------------

module System.Cron.Schedule
    ( Job (..)
    , mkJob
    , runJobs

    , ScheduleError (..)
    , Schedule
    , ScheduleT (..)

    , MonadSchedule (..)

    , runSchedule
    , runScheduleT

    , execSchedule
    ) where


-------------------------------------------------------------------------------
#if !MIN_VERSION_base(4,8,0)
import           Control.Applicative
#endif
import           Control.Concurrent
import           Control.Exception.Base
import           Control.Monad.Base
import           Control.Monad.Except
import           Control.Monad.Identity
import           Control.Monad.State
import           Control.Monad.Trans.Control
import           Data.Attoparsec.Text       (parseOnly)
import           Data.Function
import           Data.List
import           Data.Maybe
import           Data.Text                  (pack)
import           Data.Time
import           Data.Typeable
#if !MIN_VERSION_time(1,5,0)
import           System.Locale
#endif
-------------------------------------------------------------------------------
import           System.Cron.Internal.Check
import           System.Cron.Parser
import           System.Cron.Types
-------------------------------------------------------------------------------



-------------------------------------------------------------------------------
-- | Scheduling Monad
data Job m = Job CronSchedule (m ())

mkJob :: String -> m () -> Either ScheduleError (Job m)
mkJob schedStr act =
    case parseOnly cronSchedule (pack schedStr) of
        Left  e     -> Left $ ParseError e
        Right sched -> Right $ Job sched act

-------------------------------------------------------------------------------
type Jobs m = [Job m]


instance Show (Job m) where
    show (Job c _) = "(Job " ++ show c ++ ")"


-------------------------------------------------------------------------------
data ScheduleError = ParseError String
                   deriving (Show, Typeable)

instance Exception ScheduleError


-------------------------------------------------------------------------------
type Schedule = ScheduleT Identity


-------------------------------------------------------------------------------
newtype ScheduleT m a = ScheduleT { unSchedule :: StateT (Jobs IO) (ExceptT ScheduleError m) a }
        deriving ( Functor, Applicative, Monad
                 , MonadState (Jobs IO)
                 , MonadError ScheduleError
                 )


-------------------------------------------------------------------------------
runSchedule :: Schedule a -> Either ScheduleError (a, [Job IO])
runSchedule = runIdentity . runScheduleT


-------------------------------------------------------------------------------
runScheduleT :: ScheduleT m a -> m (Either ScheduleError (a, [Job IO]))
runScheduleT = runExceptT . flip runStateT [] . unSchedule


-------------------------------------------------------------------------------
class MonadSchedule m where
    addJob ::  IO () -> String -> m ()

instance (Monad m) => MonadSchedule (ScheduleT m) where
    addJob a t = either throwError (modify . (:)) $ mkJob t a


-------------------------------------------------------------------------------
-- Monitoring engine
-------------------------------------------------------------------------------


-- | Schedule all of the jobs to run at appropriate intervals. Each
-- job that is launched gets a scheduling thread to itself. Each
-- time a scheduling thread launches a job, the job is forked onto
-- a new thread. This means that if a job throws an excpetion in IO,
-- its thread will be killed, but it will continue to be scheduled
-- in the future.
execSchedule :: Schedule () -> IO [ThreadId]
execSchedule s = let res = runSchedule s
                  in case res of
                        Left  e         -> print e >> return []
                        Right (_, jobs) -> (:[]) <$> forkIO (runJobs jobs)

-------------------------------------------------------------------------------

-- | Run a set of jobs at the appropriate times. The current thread is used
-- as the manager, and will spend most of its time sleeping. When a job
-- comes due, a thread is forked to run it. If none of the given jobs will
-- ever match, this function returns immediately.
runJobs :: MonadBaseControl IO m => [Job m] -> m ()
runJobs jobs = go
  where
    go = do
        now <- liftBase getCurrentTime
        let matches = mapMaybe (\(Job sched act) -> (,) act <$> nextMatch sched now) jobs
        case sortBy (compare `on` snd) matches of
            [] -> return ()  -- none of the jobs will ever match; break
            (act,time):_ -> do
                liftBase (sleepUntil time)         -- wait until the next match
                void (liftBaseDiscard forkIO act)  -- fork the next action
                go                                 -- loop

-- | Sleep the current thread until the given time (or shortly thereafter).
sleepUntil :: UTCTime -> IO ()
sleepUntil alarm = do
    now <- getCurrentTime
    let diff = diffUTCTime alarm now
    -- NOTE: GHC doesn't guarantee that the thread will be woken up promptly,
    -- but in practice this is rarely an issue.
    threadDelay $ round $ diff * 1.0e6
