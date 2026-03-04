package com.example.dynamodb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.TableStatus;

@Testcontainers(disabledWithoutDocker = true)
class CommerceSingleTableRepositoryIT {
    private static final String TABLE_NAME = "commerce_single_table";
    private static final List<String> DEBUG_COLUMNS = List.of(
            "PK",
            "SK",
            "entityType",
            "status",
            "itemStatus",
            "version",
            "GSI1PK",
            "GSI1SK",
            "GSI2PK",
            "GSI2SK",
            "GSI3PK",
            "GSI3SK",
            "oldStatus",
            "newStatus",
            "previousVersion",
            "newVersion",
            "changedAt");

    @Container
    static final GenericContainer<?> DYNAMODB_LOCAL = new GenericContainer<>(
            DockerImageName.parse("amazon/dynamodb-local:latest"))
            .withExposedPorts(8000)
            .withCommand("-jar", "DynamoDBLocal.jar", "-inMemory", "-sharedDb");

    private static DynamoDbClient dynamoDbClient;
    private static CommerceSingleTableRepository repository;

    @BeforeAll
    static void setUp() {
        URI endpoint = URI.create("http://" + DYNAMODB_LOCAL.getHost() + ":" + DYNAMODB_LOCAL.getMappedPort(8000));

        dynamoDbClient = DynamoDbClient.builder()
                .endpointOverride(endpoint)
                .region(Region.US_EAST_1)
                .credentialsProvider(
                        StaticCredentialsProvider.create(AwsBasicCredentials.create("dummy", "dummy")))
                .build();

        repository = new CommerceSingleTableRepository(dynamoDbClient, TABLE_NAME);
        repository.createTableIfMissing();
        waitForTableToBecomeActive();
    }

    @AfterAll
    static void tearDown() {
        if (dynamoDbClient != null) {
            dynamoDbClient.close();
        }
    }

    @Test
    void shouldLoadOrderAggregateFromSinglePartitionQuery() {
        // Access pattern:
        // "Given customerId+orderId, return order root + all order items in one Query."
        String customerId = "cust-" + UUID.randomUUID();
        String orderId = "ord-100";

        repository.putCustomer(new CustomerProfile(customerId, "Alice", "alice@example.com"));
        repository.putOrder(new CustomerOrder(
                customerId,
                orderId,
                Instant.parse("2026-01-15T10:15:30Z"),
                OrderStatus.PAID,
                12_999));
        repository.putOrderItem(customerId, new OrderLineItem(orderId, "item-1", "SKU-TSHIRT", 2, 4_999, OrderItemStatus.PENDING));
        repository.putOrderItem(customerId, new OrderLineItem(orderId, "item-2", "SKU-STICKER", 1, 3_001, OrderItemStatus.PENDING));

        var aggregateOpt = repository.getOrderWithItems(customerId, orderId);

        // We expect the aggregate to exist because we inserted both the root ORDER row and ITEM child rows.
        assertTrue(aggregateOpt.isPresent());
        OrderAggregate aggregate = aggregateOpt.get();
        // The root ORDER payload should round-trip unchanged through a single-partition query.
        assertEquals(orderId, aggregate.order().orderId());
        assertEquals(OrderStatus.PAID, aggregate.order().status());
        // Both line items should be returned and keep deterministic SK ordering (item-1 then item-2).
        assertEquals(2, aggregate.items().size());
        assertEquals("item-1", aggregate.items().get(0).itemId());
        assertEquals("item-2", aggregate.items().get(1).itemId());
        // Item-level status is stored on each ORDER_ITEM row and should be preserved.
        assertEquals(OrderItemStatus.PENDING, aggregate.items().get(0).itemStatus());
    }

    @Test
    void shouldGetCustomerProfileByPrimaryKey() {
        String customerId = "cust-" + UUID.randomUUID();
        repository.putCustomer(new CustomerProfile(customerId, "Nina", "nina@example.com"));

        var profileOpt = repository.getCustomerProfile(customerId);

        // Exact PK/SK lookup should return the profile row we just wrote.
        assertTrue(profileOpt.isPresent());
        assertEquals("Nina", profileOpt.get().name());
        assertEquals("nina@example.com", profileOpt.get().email());
    }

    @Test
    void shouldUpsertCustomerProfileByExactKey() {
        String customerId = "cust-" + UUID.randomUUID();
        repository.putCustomer(new CustomerProfile(customerId, "Old Name", "old@example.com"));
        repository.putCustomer(new CustomerProfile(customerId, "New Name", "new@example.com"));

        var profileOpt = repository.getCustomerProfile(customerId);

        // Plain PutItem on same PK/SK overwrites prior values; latest write should win.
        assertTrue(profileOpt.isPresent());
        assertEquals("New Name", profileOpt.get().name());
        assertEquals("new@example.com", profileOpt.get().email());
    }

    @Test
    void shouldGetOrderHeaderByPrimaryKey() {
        String customerId = "cust-" + UUID.randomUUID();
        String orderId = "ord-header";
        repository.putOrder(new CustomerOrder(
                customerId,
                orderId,
                Instant.parse("2026-01-10T09:00:00Z"),
                OrderStatus.PAID,
                8_500));

        var orderOpt = repository.getOrderHeader(customerId, orderId);

        // Order header is a point read on PK/SK, so the inserted order must be present.
        assertTrue(orderOpt.isPresent());
        assertEquals(orderId, orderOpt.get().orderId());
        assertEquals(OrderStatus.PAID, orderOpt.get().status());
    }

    @Test
    void shouldGetOrderItemByPrimaryKey() {
        String customerId = "cust-" + UUID.randomUUID();
        String orderId = "ord-get-item";
        repository.putOrder(new CustomerOrder(
                customerId,
                orderId,
                Instant.parse("2026-01-11T09:00:00Z"),
                OrderStatus.PAID,
                9_000));
        repository.putOrderItem(customerId, new OrderLineItem(orderId, "item-1", "SKU-MUG", 1, 9_000, OrderItemStatus.PENDING));

        var itemOpt = repository.getOrderItem(customerId, orderId, "item-1");

        // ORDER_ITEM is addressable by full PK/SK; we should get back that exact row.
        assertTrue(itemOpt.isPresent());
        assertEquals("SKU-MUG", itemOpt.get().sku());
        assertEquals(OrderItemStatus.PENDING, itemOpt.get().itemStatus());
    }

    @Test
    void shouldListOrderItemsForOneOrderByPrefixQuery() {
        String customerId = "cust-" + UUID.randomUUID();
        String orderId = "ord-list-items";
        repository.putOrder(new CustomerOrder(
                customerId,
                orderId,
                Instant.parse("2026-01-12T09:00:00Z"),
                OrderStatus.PAID,
                13_000));
        repository.putOrderItem(customerId, new OrderLineItem(orderId, "item-1", "SKU-A", 1, 5_000, OrderItemStatus.PENDING));
        repository.putOrderItem(customerId, new OrderLineItem(orderId, "item-2", "SKU-B", 1, 8_000, OrderItemStatus.PICKED));

        List<OrderLineItem> items = repository.listOrderItems(customerId, orderId);

        // Prefix query should include all ORDER_ITEM rows for this order and exclude everything else.
        assertEquals(2, items.size());
        assertEquals("item-1", items.get(0).itemId());
        assertEquals("item-2", items.get(1).itemId());
    }

    @Test
    void shouldQueryCustomerOrderTimelineFromGsi() {
        // Access pattern:
        // "Given customerId, list all orders newest first."
        String customerId = "cust-" + UUID.randomUUID();

        repository.putOrder(new CustomerOrder(
                customerId,
                "ord-old",
                Instant.parse("2026-01-01T09:00:00Z"),
                OrderStatus.CREATED,
                5_000));
        repository.putOrder(new CustomerOrder(
                customerId,
                "ord-new",
                Instant.parse("2026-01-02T09:00:00Z"),
                OrderStatus.CREATED,
                7_500));

        List<CustomerOrder> orders = repository.listOrdersForCustomerNewestFirst(customerId, 10);

        // GSI1 sort key embeds createdAt, so descending query returns newest order first.
        assertEquals(2, orders.size());
        assertEquals("ord-new", orders.get(0).orderId());
        assertEquals("ord-old", orders.get(1).orderId());
    }

    @Test
    void shouldPaginateCustomerTimelineUsingExclusiveStartKey() {
        String customerId = "cust-" + UUID.randomUUID();
        repository.putOrder(new CustomerOrder(
                customerId,
                "ord-1",
                Instant.parse("2026-01-01T09:00:00Z"),
                OrderStatus.CREATED,
                2_000));
        repository.putOrder(new CustomerOrder(
                customerId,
                "ord-2",
                Instant.parse("2026-01-02T09:00:00Z"),
                OrderStatus.CREATED,
                3_000));
        repository.putOrder(new CustomerOrder(
                customerId,
                "ord-3",
                Instant.parse("2026-01-03T09:00:00Z"),
                OrderStatus.CREATED,
                4_000));

        CustomerOrderPage page1 = repository.listOrdersForCustomerNewestFirstPage(customerId, 2, Map.of());
        CustomerOrderPage page2 = repository.listOrdersForCustomerNewestFirstPage(customerId, 2, page1.lastEvaluatedKey());

        // First page should return the two newest rows and a continuation token.
        assertEquals(2, page1.orders().size());
        assertFalse(page1.lastEvaluatedKey().isEmpty());
        assertEquals("ord-3", page1.orders().get(0).orderId());
        assertEquals("ord-2", page1.orders().get(1).orderId());

        // Second page should continue from token and return only the remaining oldest order.
        assertEquals(1, page2.orders().size());
        assertEquals("ord-1", page2.orders().get(0).orderId());
    }

    @Test
    void shouldQueryOrdersByStatusAcrossCustomersFromGsi() {
        // Access pattern:
        // "Given status, list orders across all customers newest first."
        repository.putOrder(new CustomerOrder(
                "cust-" + UUID.randomUUID(),
                "ord-status-old",
                Instant.parse("2026-02-01T09:00:00Z"),
                OrderStatus.FULFILLED,
                4_000));
        repository.putOrder(new CustomerOrder(
                "cust-" + UUID.randomUUID(),
                "ord-status-new",
                Instant.parse("2026-02-02T09:00:00Z"),
                OrderStatus.FULFILLED,
                8_000));
        repository.putOrder(new CustomerOrder(
                "cust-" + UUID.randomUUID(),
                "ord-other-status",
                Instant.parse("2026-02-03T09:00:00Z"),
                OrderStatus.CANCELLED,
                8_000));

        List<CustomerOrder> fulfilledOrders = repository.listOrdersByStatusNewestFirst(OrderStatus.FULFILLED, 10);

        // GSI2 partitions by status, so CANCELLED row is excluded and FULFILLED rows are time-sorted.
        assertEquals(2, fulfilledOrders.size());
        assertEquals("ord-status-new", fulfilledOrders.get(0).orderId());
        assertEquals("ord-status-old", fulfilledOrders.get(1).orderId());
    }

    @Test
    void shouldQueryOrdersByStatusForSingleCustomerFromGsi() {
        // Access pattern:
        // "Given customerId+status, list matching orders newest first."
        String customerId = "cust-" + UUID.randomUUID();

        repository.putOrder(new CustomerOrder(
                customerId,
                "ord-created",
                Instant.parse("2026-03-01T09:00:00Z"),
                OrderStatus.CREATED,
                4_500));
        repository.putOrder(new CustomerOrder(
                customerId,
                "ord-fulfilled-old",
                Instant.parse("2026-03-02T09:00:00Z"),
                OrderStatus.FULFILLED,
                6_000));
        repository.putOrder(new CustomerOrder(
                customerId,
                "ord-fulfilled-new",
                Instant.parse("2026-03-03T09:00:00Z"),
                OrderStatus.FULFILLED,
                7_000));

        List<CustomerOrder> fulfilledForCustomer =
                repository.listOrdersForCustomerByStatusNewestFirst(customerId, OrderStatus.FULFILLED, 10);

        // GSI3 key includes customer+status, so only this customer's fulfilled orders should match.
        assertEquals(2, fulfilledForCustomer.size());
        assertEquals("ord-fulfilled-new", fulfilledForCustomer.get(0).orderId());
        assertEquals("ord-fulfilled-old", fulfilledForCustomer.get(1).orderId());
    }

    @Test
    void shouldUpdateOrderStatusAndMoveOrderAcrossStatusIndexes() {
        String customerId = "cust-" + UUID.randomUUID();
        String orderId = "ord-status-move";

        repository.putOrder(new CustomerOrder(
                customerId,
                orderId,
                Instant.parse("2026-03-04T09:00:00Z"),
                OrderStatus.CREATED,
                9_000));

        // Before transition, order should be discoverable in CREATED buckets.
        assertEquals(1, repository.listOrdersByStatusNewestFirst(OrderStatus.CREATED, 10).stream()
                .filter(o -> orderId.equals(o.orderId()))
                .count());
        assertEquals(1, repository.listOrdersForCustomerByStatusNewestFirst(customerId, OrderStatus.CREATED, 10).stream()
                .filter(o -> orderId.equals(o.orderId()))
                .count());

        repository.updateOrderStatus(customerId, orderId, OrderStatus.FULFILLED);

        // After transition, order must disappear from old status and appear in new status on both GSIs.
        assertEquals(0, repository.listOrdersByStatusNewestFirst(OrderStatus.CREATED, 10).stream()
                .filter(o -> orderId.equals(o.orderId()))
                .count());
        assertEquals(0, repository.listOrdersForCustomerByStatusNewestFirst(customerId, OrderStatus.CREATED, 10).stream()
                .filter(o -> orderId.equals(o.orderId()))
                .count());
        assertEquals(1, repository.listOrdersByStatusNewestFirst(OrderStatus.FULFILLED, 10).stream()
                .filter(o -> orderId.equals(o.orderId()))
                .count());
        assertEquals(1, repository.listOrdersForCustomerByStatusNewestFirst(customerId, OrderStatus.FULFILLED, 10).stream()
                .filter(o -> orderId.equals(o.orderId()))
                .count());
    }

    @Test
    void shouldDeleteOrderItemByPrimaryKey() {
        String customerId = "cust-" + UUID.randomUUID();
        String orderId = "ord-delete-item";
        repository.putOrder(new CustomerOrder(
                customerId,
                orderId,
                Instant.parse("2026-03-04T12:00:00Z"),
                OrderStatus.PAID,
                1_000));
        repository.putOrderItem(customerId, new OrderLineItem(orderId, "item-1", "SKU-DEL", 1, 1_000, OrderItemStatus.PENDING));

        repository.deleteOrderItem(customerId, orderId, "item-1");

        // Exact-key delete should remove both direct lookup visibility and prefix-query visibility.
        assertTrue(repository.getOrderItem(customerId, orderId, "item-1").isEmpty());
        assertEquals(0, repository.listOrderItems(customerId, orderId).size());
    }

    @Test
    void shouldUseConditionalWriteForIdempotentCustomerCreate() {
        String customerId = "cust-" + UUID.randomUUID();
        boolean firstInsert = repository.putCustomerIfAbsent(new CustomerProfile(customerId, "Alice", "alice+1@example.com"));
        boolean secondInsert = repository.putCustomerIfAbsent(new CustomerProfile(customerId, "Alice Changed", "alice+2@example.com"));

        var profileOpt = repository.getCustomerProfile(customerId);
        // Conditional put should allow first insert and reject duplicate key on second insert.
        assertTrue(firstInsert);
        assertFalse(secondInsert);
        // Because second insert failed, stored value should still be from first insert.
        assertTrue(profileOpt.isPresent());
        assertEquals("alice+1@example.com", profileOpt.get().email());
    }

    @Test
    void shouldCreateOrderAndItemsAtomically() {
        String customerId = "cust-" + UUID.randomUUID();
        String orderId = "ord-tx-ok";
        CustomerOrder order = new CustomerOrder(
                customerId,
                orderId,
                Instant.parse("2026-03-05T12:00:00Z"),
                OrderStatus.PAID,
                15_000);
        List<OrderLineItem> items = List.of(
                new OrderLineItem(orderId, "item-1", "SKU-TX1", 1, 5_000, OrderItemStatus.PENDING),
                new OrderLineItem(orderId, "item-2", "SKU-TX2", 1, 10_000, OrderItemStatus.PENDING));

        boolean created = repository.createOrderWithItemsAtomic(order, items);
        var aggregateOpt = repository.getOrderWithItems(customerId, orderId);

        // Transaction success means root and all child rows are committed together.
        assertTrue(created);
        assertTrue(aggregateOpt.isPresent());
        assertEquals(2, aggregateOpt.get().items().size());
    }

    @Test
    void shouldRollbackAtomicCreateWhenAnyRowAlreadyExists() {
        String customerId = "cust-" + UUID.randomUUID();
        String orderId = "ord-tx-fail";
        repository.putOrderItem(
                customerId,
                new OrderLineItem(orderId, "item-1", "SKU-EXISTS", 1, 3_000, OrderItemStatus.PENDING));

        CustomerOrder order = new CustomerOrder(
                customerId,
                orderId,
                Instant.parse("2026-03-06T12:00:00Z"),
                OrderStatus.PAID,
                9_000);
        List<OrderLineItem> items = List.of(
                new OrderLineItem(orderId, "item-1", "SKU-EXISTS", 1, 3_000, OrderItemStatus.PENDING),
                new OrderLineItem(orderId, "item-2", "SKU-NEW", 1, 6_000, OrderItemStatus.PENDING));

        boolean created = repository.createOrderWithItemsAtomic(order, items);

        // Existing conflicting row should cancel whole transaction (no partial writes).
        assertFalse(created);
        // Root ORDER must not exist because transaction rolled back.
        assertTrue(repository.getOrderHeader(customerId, orderId).isEmpty());
        // Pre-existing conflicting item remains; new item from failed transaction must not be created.
        assertEquals(1, repository.listOrderItems(customerId, orderId).size());
        assertTrue(repository.getOrderItem(customerId, orderId, "item-2").isEmpty());
    }

    @Test
    void shouldAppendStatusHistoryAndIncrementVersionOnStatusUpdate() {
        String customerId = "cust-" + UUID.randomUUID();
        String orderId = "ord-history-success";
        repository.putOrder(new CustomerOrder(
                customerId,
                orderId,
                Instant.parse("2026-03-07T12:00:00Z"),
                OrderStatus.CREATED,
                12_000));

        long version1 = repository.getOrderVersion(customerId, orderId);
        boolean updated = repository.updateOrderStatusWithHistory(
                customerId,
                orderId,
                OrderStatus.IN_FULFILLMENT,
                version1,
                "worker-1",
                "picked-for-fulfillment");

        // Correct expectedVersion should allow update and increment optimistic-lock version.
        assertTrue(updated);
        assertEquals(2L, repository.getOrderVersion(customerId, orderId));
        // Current order state should reflect the new status after successful transaction.
        assertEquals(OrderStatus.IN_FULFILLMENT, repository.getOrderHeader(customerId, orderId).orElseThrow().status());

        List<OrderStatusHistoryEvent> history = repository.listOrderStatusHistoryNewestFirst(customerId, orderId, 10);
        // Exactly one immutable audit event should be appended with full transition metadata.
        assertEquals(1, history.size());
        assertEquals(OrderStatus.CREATED, history.get(0).oldStatus());
        assertEquals(OrderStatus.IN_FULFILLMENT, history.get(0).newStatus());
        assertEquals("worker-1", history.get(0).changedBy());
        assertEquals(1L, history.get(0).previousVersion());
        assertEquals(2L, history.get(0).newVersion());
    }

    @Test
    void shouldRejectStaleVersionDuringOptimisticStatusUpdate() {
        String customerId = "cust-" + UUID.randomUUID();
        String orderId = "ord-history-stale";
        repository.putOrder(new CustomerOrder(
                customerId,
                orderId,
                Instant.parse("2026-03-08T12:00:00Z"),
                OrderStatus.CREATED,
                14_000));

        long staleVersion = repository.getOrderVersion(customerId, orderId);
        boolean firstUpdate = repository.updateOrderStatusWithHistory(
                customerId,
                orderId,
                OrderStatus.PAID,
                staleVersion,
                "payments-service",
                "payment-captured");
        boolean secondUpdateWithStaleVersion = repository.updateOrderStatusWithHistory(
                customerId,
                orderId,
                OrderStatus.CANCELLED,
                staleVersion,
                "ops-user",
                "late-cancel");

        // First writer wins; second writer using stale version must be rejected by condition check.
        assertTrue(firstUpdate);
        assertFalse(secondUpdateWithStaleVersion);
        // Final state should remain from the successful first update.
        assertEquals(OrderStatus.PAID, repository.getOrderHeader(customerId, orderId).orElseThrow().status());
        assertEquals(2L, repository.getOrderVersion(customerId, orderId));

        List<OrderStatusHistoryEvent> history = repository.listOrderStatusHistoryNewestFirst(customerId, orderId, 10);
        // Rejected stale write must not append a second history event.
        assertEquals(1, history.size());
        assertEquals(OrderStatus.CREATED, history.get(0).oldStatus());
        assertEquals(OrderStatus.PAID, history.get(0).newStatus());
    }

    @Test
    void shouldUpdateItemStatusWithinOrder() {
        String customerId = "cust-" + UUID.randomUUID();
        String orderId = "ord-item-status";

        repository.putOrder(new CustomerOrder(
                customerId,
                orderId,
                Instant.parse("2026-03-05T09:00:00Z"),
                OrderStatus.PAID,
                11_000));
        repository.putOrderItem(customerId, new OrderLineItem(orderId, "item-1", "SKU-BOOK", 1, 11_000, OrderItemStatus.PENDING));

        repository.updateOrderItemStatus(customerId, orderId, "item-1", OrderItemStatus.SHIPPED);

        var aggregateOpt = repository.getOrderWithItems(customerId, orderId);
        // Item-level mutation should affect only that item and be visible in aggregate read.
        assertTrue(aggregateOpt.isPresent());
        assertEquals(OrderItemStatus.SHIPPED, aggregateOpt.get().items().get(0).itemStatus());
    }

    private static void printTableSnapshot(String title) {
        List<Map<String, AttributeValue>> items = new ArrayList<>(dynamoDbClient.scan(
                        ScanRequest.builder().tableName(TABLE_NAME).build())
                .items());

        items.sort(Comparator
                .comparing((Map<String, AttributeValue> item) -> attr(item, "PK"))
                .thenComparing(item -> attr(item, "SK")));

        List<List<String>> rows = new ArrayList<>();
        rows.add(DEBUG_COLUMNS);
        for (Map<String, AttributeValue> item : items) {
            List<String> row = new ArrayList<>();
            for (String column : DEBUG_COLUMNS) {
                row.add(attr(item, column));
            }
            rows.add(row);
        }

        int[] widths = new int[DEBUG_COLUMNS.size()];
        for (List<String> row : rows) {
            for (int i = 0; i < row.size(); i++) {
                widths[i] = Math.max(widths[i], row.get(i).length());
            }
        }

        System.out.println();
        System.out.println("=== " + title + " ===");
        printBorder(widths);
        printRow(DEBUG_COLUMNS, widths);
        printBorder(widths);
        for (int i = 1; i < rows.size(); i++) {
            printRow(rows.get(i), widths);
        }
        printBorder(widths);
    }

    private static void printBorder(int[] widths) {
        StringBuilder sb = new StringBuilder("+");
        for (int width : widths) {
            sb.append("-".repeat(width + 2)).append("+");
        }
        System.out.println(sb);
    }

    private static void printRow(List<String> row, int[] widths) {
        StringBuilder sb = new StringBuilder("|");
        for (int i = 0; i < row.size(); i++) {
            sb.append(" ").append(padRight(row.get(i), widths[i])).append(" |");
        }
        System.out.println(sb);
    }

    private static String padRight(String value, int width) {
        return String.format("%1$-" + width + "s", value);
    }

    private static String attr(Map<String, AttributeValue> item, String key) {
        AttributeValue value = item.get(key);
        if (value == null) {
            return "";
        }
        if (value.s() != null) {
            return value.s();
        }
        if (value.n() != null) {
            return value.n();
        }
        if (value.bool() != null) {
            return value.bool().toString();
        }
        if (value.hasSs() && !value.ss().isEmpty()) {
            return String.join(",", value.ss());
        }
        if (value.hasNs() && !value.ns().isEmpty()) {
            return String.join(",", value.ns());
        }
        return value.toString();
    }

    private static void waitForTableToBecomeActive() {
        long timeoutMs = Duration.ofSeconds(10).toMillis();
        long start = System.currentTimeMillis();

        while (System.currentTimeMillis() - start < timeoutMs) {
            try {
                var table = dynamoDbClient.describeTable(
                                DescribeTableRequest.builder().tableName(TABLE_NAME).build())
                        .table();
                if (table.tableStatus() == TableStatus.ACTIVE) {
                    return;
                }
            } catch (ResourceNotFoundException ignored) {
            }

            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted while waiting for table to become active", e);
            }
        }

        throw new IllegalStateException("Table did not become ACTIVE within timeout");
    }
}
