Neyro.Data
==========
Small and fast micro ORM.

Full description you can see in http://habrahabr.ru/post/218225/


Usage:
Create data manager for MS Sql:

```c#
class MSSqlDataManager : DataManager 
{
    public MSSqlDataManager() : base(new SqlConnection("ConnectionString here")) { }
}
```

Model:

```c#
public class Product
{
        public int Id { get; set; }
        public string Name { get; set; }
        public string Description { get; set; }
        public int? Price { get; set; }
}
```

You can get data for model from SP:
SP Code:

```sql
...
SELECT p.Id, p.Name, p.[Description], p.Price
    FROM dbo.Product p
```

C# Code:    

```c#
using (var dm = new MSSqlDataManager())
{
    List<Product> res = dm.Procedure("Test").GetList<Product>();
}
```

Next example:
You have SP:

```sql
SELECT 
        p.Id
        , p.Name, 
        , p.[Description]
        , p.Price
        , StorageId = s.Id
        , StorageName = s.Name
    FROM dbo.Product p 
    INNER JOIN dbo.Storages s ON s.Id = p.StorageId
    WHERE p.Id = @Id;
    
    SELECT 
        c.Id
        , c.Body
        , c.WriteDate
        , UserId = u.Id
        , UserName = u.Name
        , UserLocationId = l.Id
        , UserLocationName = l.Name
                          , c.ProductId
    FROM dbo.Comments c 
    INNER JOIN dbo.Users u ON u.Id = c.UserId
    INNER JOIN dbo.Locations l ON l.Id = u.LocationId 
    WHERE c.ProductId = @Id;
```  

And you have models:

```c#
public class UserLocation
    {
        public int Id { get; set; }
        public string Name { get; set; }
    }

    public class UserModel
    {
        public int Id { get; set; }
        public string Name { get; set; }
        public UserLocation Location { get; set; }

        public UserModel()
        {
            this.Location = new UserLocation();
        }
    }
    
    public class ProductComment
    {
        public int Id { get; set; }
        public string Body { get; set; }
        public DateTime WriteDate { get; set; }
        public UserModel User { get; set; }
        public int ProductId { get; set; }

        public ProductComment()
        {
            this.User = new UserModel();
        }
    }

    public class ProductStorage
    {
        public int Id { get; set; }
        public string Name { get; set; }
    }

    public class Product
    {
        public int Id { get; set; }
        public string Name { get; set; }
        public string Description { get; set; }
        public int? Price { get; set; }
        public ProductStorage Storage { get; set; }
        public List<ProductComment> Comments { get; set; }

        public Product()
        {
            this.Storage = new ProductStorage();
            this.Comments = new List<ProductComment>();
        }
    }
```

You can get data:

```c#
Product res = dm.Procedure("Test").AddParams(new { id = 10 }).Get<Product, ProductComment>(p => p.Comments);
```

You can get master-detail data:

```c#
List<Product> res = dm.Procedure("Test")
      .GetList<Product, ProductComment>(
        (parents, detail)=>parents.First(p => p.Id == detail.ProductId).Comments
      );
```

You can use TVP params (for Ms Sql only):

```c#
dm.AddEnumerableParam("Details",
                    Enumerable.Range(1, 10)
                        .Select(e => new {id = e, name = string.Concat("Name", e.ToString())})
                    );
```

And a lot of other things.

Sorry for my English :)
