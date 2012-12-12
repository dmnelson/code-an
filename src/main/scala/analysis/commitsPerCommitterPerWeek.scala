package co.torri.dod.analysis

import spark._
import com.thoughtworks.dod._


class CommitsPerCommitterPerWeekAnalyzer extends Analyzer {
    def apply(data: RepoData, sc: SparkContext) = {

        val commits = data.commits
            .map { _.yearWeek }
            .groupBy { case (date) => (date) }
            .map { case (date, i) => (date, i.size) }
            .toArray
            .sortBy { case (date, commits) => (date) }
            .toMap

        val committers = data.commits
            .flatMap { case c => c.files.flatMap(fc => fc.workspace).map(ws => (c.yearWeek, c.author)) }
            .groupBy { case (date, author) => (date) }
            .map { case (date, i) => (date, i.distinct.size) }
            .toArray
            .sortBy { case (date, commiters) => (date) }
            .map { case ((year, week), commiters) => List(year, week, commits((year, week)) * 1.0 / commiters) }

        Result(Seq("year", "week", "avg_commits_by_comitter"), committers)
    }
}
