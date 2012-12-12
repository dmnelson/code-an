package co.torri.dod.analysis

import spark._
import com.thoughtworks.dod._


class CommitsPerWeekAnalyzer extends Analyzer {
    def apply(data: RepoData, sc: SparkContext) = {

        val d = data.commits
            .flatMap { case c => c.files.flatMap(fc => fc.workspace).map(ws => (c.yearWeek)) }
            .groupBy { case (date) => (date) }
            .map { case (date, i) => (date, i.size) }
            .toArray
            .sortBy { case (date, commits) => (date) }
            .map { case ((year, week), commits) => List(year, week, commits)
            }

        Result(Seq("year", "week", "commits"), d)
    }
}
