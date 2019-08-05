using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Terrasoft.Core;
using Terrasoft.Core.DB;
using Terrasoft.Core.Entities;

namespace MyFirstLibrary
{
    public class BookshopInfoHandlerImplementation
    {
        private UserConnection _userConnection;

        public BookshopInfoHandlerImplementation(UserConnection uc) { _userConnection = uc; }

        public string CopyBookshopEntry(Guid bookId)
        {
            // copying book from the section
            var newId = Guid.NewGuid();
            var query = new Select(_userConnection)
                                .Column(Column.Const(newId))
                                .Column("UsrName")
                                .Column("UsrAuthorName")
                                .Column("UsrBookRating")
                                .Column("UsrReleaseData")
                                .Column("UsrIsInStock")
                                .From("UsrBookSectionV3")
                                .Where("Id").IsEqual(Column.Parameter(bookId))
                                as Select;
            var newBookEntry = new InsertSelect(_userConnection)
                                    .Into("UsrBookSectionV3")
                                        .Set("Id")
                                        .Set("UsrName")
                                        .Set("UsrAuthorName")
                                        .Set("UsrBookRating")
                                        .Set("UsrReleaseData")
                                        .Set("UsrIsInStock")
                                            .FromSelect(query);
            newBookEntry.Execute();


            var dateNewEntry = new Select(_userConnection)
                                        .Column("CreatedOn")
                                        .From("UsrBookSectionV3")
                                        .OrderByDesc("CreatedOn") as Select;
            var maxDate = dateNewEntry.ExecuteScalar<DateTime>();

            var selectEntryWithMaxDate = new Select(_userConnection)
                                                .Column("Id")
                                                .From("UsrBookSectionV3")
                                                .Where("CreatedOn").IsEqual(Column.Parameter(maxDate)) as Select;
            var idNewEntry = selectEntryWithMaxDate.ExecuteScalar<string>();

            var selectDetail = new Select(_userConnection)
                                    .Column("UsrAlphabetLetter")
                                    .Column("UsrBookCount")
                                    .Column(Column.Const(newId))
                                    .From("UsrBookShelf")
                                    .Where("UsrLookupBookSectionId").IsEqual(Column.Parameter(bookId)) as Select;
            var insertEntry = new InsertSelect(_userConnection)
                                    .Into("UsrBookShelf")
                                    .Set("UsrAlphabetLetter")
                                    .Set("UsrBookCount")
                                    .Set("UsrLookupBookSectionId")
                                    .FromSelect(selectDetail);
            insertEntry.Execute();

            return string.Empty;
        }

        public string CopyBookshopEntryUsingESM(Guid bookId)
        {
            var gistManager = _userConnection.EntitySchemaManager.GetInstanceByName("UsrBookSectionV3");
            var entityDetail = _userConnection.EntitySchemaManager.GetInstanceByName("UsrBookShelf");

            var newId = Guid.NewGuid();

            var fieldFrom = gistManager.CreateEntity(_userConnection);
            var fieldTo = gistManager.CreateEntity(_userConnection);

            if (fieldFrom.FetchFromDB(bookId))
            {
                fieldTo.SetColumnValue("Id", newId);
                fieldTo.SetColumnValue("UsrName", fieldFrom.GetColumnValue("UsrName"));
                fieldTo.SetColumnValue("UsrAuthorName", fieldFrom.GetColumnValue("UsrAuthorName"));
                fieldTo.SetColumnValue("UsrBookRating", fieldFrom.GetColumnValue("UsrBookRating"));
                fieldTo.SetColumnValue("UsrReleaseData", fieldFrom.GetColumnValue("UsrReleaseData"));
                fieldTo.SetColumnValue("UsrIsInStock", fieldFrom.GetColumnValue("UsrIsInStock"));
                fieldTo.Save();
            }

            EntitySchemaManager esqManager = _userConnection.EntitySchemaManager;
            var esqResult = new EntitySchemaQuery(esqManager, "UsrBookShelf");
            esqResult.AddColumn("Id");
            esqResult.AddColumn("UsrLookupBookSection.Id");
            esqResult.AddColumn("UsrAlphabetLetter");
            esqResult.AddColumn("UsrBookCount");
            var filter = esqResult.CreateFilterWithParameters(FilterComparisonType.Equal, "UsrLookupBookSection.Id", bookId);
            esqResult.Filters.Add(filter);

            var entities = esqResult.GetEntityCollection(_userConnection);

            foreach(var item in entities)
            {
                var detail = entityDetail.CreateEntity(_userConnection);
                detail.SetColumnValue("Id", Guid.NewGuid());
                detail.SetColumnValue("UsrLookupBookSectionId", newId);
                detail.SetColumnValue("UsrAlphabetLetter", item.GetColumnValue("UsrAlphabetLetter"));
                detail.SetColumnValue("UsrBookCount", item.GetColumnValue("UsrBookCount"));
                detail.Save();
            }
            


            return string.Empty;
        }

    }
}
