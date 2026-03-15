{
  from: "rich_messages",
  let: {
    chatId: "$id"
  },
  pipeline: [
    {
      $match: {
        $expr: {
          $eq: [
            {
              $toString: "$chat_id"
            },
            {
              $toString: "$$chatId"
            }
          ]
        }
      }
    },
    {
      $addFields: {
        msg_dt: {
          $convert: {
            input: "$created_time",
            to: "date",
            onError: null,
            onNull: null
          }
        }
      }
    },
    {
      $match: {
        $expr: {
          $gte: [
            "$msg_dt",
            {
              $dateSubtract: {
                startDate: "$$NOW",
                unit: "day",
                amount: 2
              }
            }
          ]
        }
      }
    },
    {
      $sort: {
        msg_dt: 1
      }
    },
    // mark assistant fields only on assistant rows
    {
      $addFields: {
        assistant_sql_if_any: {
          $cond: [
            {
              $eq: ["$role", "assistant"]
            },
            "$sql_response",
            null
          ]
        },
        assistant_text_if_any: {
          $cond: [
            {
              $eq: ["$role", "assistant"]
            },
            "$text_response",
            null
          ]
        },
        assistant_md_if_any: {
          $cond: [
            {
              $eq: ["$role", "assistant"]
            },
            "$content.markdown_response",
            null
          ]
        }
      }
    },
    // ONLY 2 look-ahead retries
    {
      $setWindowFields: {
        sortBy: {
          msg_dt: 1
        },
        output: {
          next_sql_1: {
            $shift: {
              output: "$assistant_sql_if_any",
              by: -1
            }
          },
          next_sql_2: {
            $shift: {
              output: "$assistant_sql_if_any",
              by: -2
            }
          },
          next_text_1: {
            $shift: {
              output: "$assistant_text_if_any",
              by: -1
            }
          },
          next_text_2: {
            $shift: {
              output: "$assistant_text_if_any",
              by: -2
            }
          },
          next_md_1: {
            $shift: {
              output: "$assistant_md_if_any",
              by: -1
            }
          },
          next_md_2: {
            $shift: {
              output: "$assistant_md_if_any",
              by: -2
            }
          }
        }
      }
    },
    // keep only user questions
    {
      $match: {
        role: "user"
      }
    },
    // pick nearest non-null from next 2
    {
      $addFields: {
        sql_response: {
          $ifNull: ["$next_sql_1", "$next_sql_2"]
        },
        text_response: {
          $ifNull: [
            "$next_text_1",
            "$next_text_2"
          ]
        },
        markdown_response: {
          $ifNull: ["$next_md_1", "$next_md_2"]
        }
      }
    },
    {
      $project: {
        _id: 0,
        asked_at: "$msg_dt",
        question: "$content",
        sql_response: 1,
        text_response: 1,
        markdown_response: 1
      }
    }
  ],
  as: "user_questions_last2d"
}
